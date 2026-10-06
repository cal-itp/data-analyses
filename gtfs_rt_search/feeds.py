"""
Resolve GTFS-RT VehiclePositions feed names from the warehouse into pollable URLs.

Deliberately does NOT use shared_utils.schedule_rt_utils.filter_dim_gtfs_datasets, which
hard-filters data_quality_pipeline == True and would silently drop feeds the user asked for
by name. Feed selection here is exactly what the config lists; unknown names are a hard error.
"""

from __future__ import annotations

import base64
import dataclasses
import math
from typing import Any, Iterable, Optional

import pandas as pd

DIM_GTFS_DATASETS = "`cal-itp-data-infra.mart_transit_database.dim_gtfs_datasets`"
DIM_PROVIDER_GTFS_DATA = "`cal-itp-data-infra.mart_transit_database.dim_provider_gtfs_data`"

# Statuses a feed can carry out of resolution. "ok" is the only pollable one.
STATUS_OK = "ok"
STATUS_SKIPPED_AUTH = "skipped_auth"
STATUS_AMBIGUOUS_NAME = "ambiguous_name"
STATUS_UNRESOLVED_URL_TEMPLATE = "unresolved_url_template"
STATUS_NO_URL = "skipped_no_url"


class FeedResolutionError(ValueError):
    """Raised when requested feed names cannot be resolved to warehouse rows."""


@dataclasses.dataclass(frozen=True)
class FeedTarget:
    """One resolved feed. Only status == "ok" entries are polled."""

    feed_name: str
    status: str
    url: Optional[str] = None
    gtfs_dataset_key: Optional[str] = None
    is_regional: bool = False
    organization_names: tuple[str, ...] = ()
    n_organizations: int = 0
    regional_feed_type: Optional[str] = None
    private_dataset: bool = False
    deprecated_date: Optional[str] = None
    duplicate_of: Optional[str] = None
    notes: tuple[str, ...] = ()

    @property
    def is_pollable(self) -> bool:
        return self.status == STATUS_OK and bool(self.url)


def _is_true(value: Any) -> bool:
    """
    Truthiness for a nullable BigQuery BOOL.

    has_authentication is never False in the warehouse - it is True or NULL - and
    bool(float("nan")) is True in Python, so a plain `if row.has_authentication:` would skip
    every feed. NaN, None and empty strings all mean "not set" here.
    """
    if value is None:
        return False
    if isinstance(value, float) and math.isnan(value):
        return False
    if isinstance(value, str):
        return value.strip().lower() in {"true", "t", "1", "yes", "y"}
    return bool(value)


def _is_missing(value: Any) -> bool:
    """True when a warehouse value is NULL/NaN/blank."""
    if value is None:
        return True
    if isinstance(value, float) and math.isnan(value):
        return True
    if isinstance(value, str) and not value.strip():
        return True
    return False


def _quote_name(name: str) -> str:
    """
    Quote a feed name for a BigQuery string literal.

    query_sql takes no bind parameters, so the IN list is interpolated. Anything that could
    break out of the literal is rejected rather than escaped.
    """
    if "'" in name or "\\" in name or "\n" in name or "\r" in name:
        raise FeedResolutionError(f"feed name contains an unsupported character (quote/backslash/newline): {name!r}")
    return f"'{name}'"


def build_query(names: Iterable[str]) -> str:
    """
    SQL for the requested feed names.

    The provider table is pre-aggregated in its own CTE: dim_provider_gtfs_data is many-to-many
    against a dataset key, so joining it raw and deduping afterwards would fan out the rows.

    Note the IN list is interpolated here rather than left as a Jinja placeholder - query_sql
    renders its SQL through Jinja with only USER_DEFINED_MACROS in context, so a `{{ names }}`
    placeholder would render empty and produce `IN ()`.
    """
    quoted = ", ".join(_quote_name(n) for n in names)
    if not quoted:
        raise FeedResolutionError("no feed names given")
    return f"""
WITH ds AS (
  SELECT key, name, uri, base64_url, has_authentication, private_dataset,
         deprecated_date, data_quality_pipeline, regional_feed_type
  FROM {DIM_GTFS_DATASETS}
  WHERE _is_current AND type = 'vehicle_positions' AND name IN ({quoted})
), prov AS (
  SELECT vehicle_positions_gtfs_dataset_key AS key,
         ARRAY_AGG(DISTINCT organization_name IGNORE NULLS ORDER BY organization_name LIMIT 50) AS organization_names,
         ARRAY_AGG(DISTINCT regional_feed_type IGNORE NULLS LIMIT 10) AS regional_feed_types,
         LOGICAL_OR(gtfs_service_data_customer_facing) AS any_customer_facing,
         COUNT(DISTINCT organization_name) AS n_organizations
  FROM {DIM_PROVIDER_GTFS_DATA}
  WHERE _is_current AND vehicle_positions_gtfs_dataset_key IS NOT NULL
  GROUP BY 1
)
SELECT ds.*, prov.* EXCEPT (key)
FROM ds LEFT JOIN prov USING (key)
""".strip()


def fetch_feed_metadata(names: Iterable[str], engine=None, max_bytes: Optional[int] = None) -> pd.DataFrame:
    """
    Run the resolution query against BigQuery.

    Requires application-default credentials; run `glogin` first. Imported lazily so the pure
    resolution logic below stays testable without the warehouse stack.
    """
    from calitp_data_analysis.sql import get_engine, query_sql

    if engine is None:
        engine = get_engine(max_bytes=max_bytes) if max_bytes is not None else get_engine()
    return query_sql(build_query(names), engine=engine)


def _decode_base64_url(encoded: str) -> Optional[str]:
    """Decode a base64url-encoded feed URL, tolerating missing padding."""
    try:
        padded = encoded + "=" * (-len(encoded) % 4)
        return base64.urlsafe_b64decode(padded).decode("utf-8").strip() or None
    except (ValueError, UnicodeDecodeError):
        return None


def _resolve_url(row: pd.Series) -> tuple[Optional[str], tuple[str, ...]]:
    """Prefer uri; fall back to the base64url form when uri is NULL."""
    notes: list[str] = []
    uri = row.get("uri")
    if not _is_missing(uri):
        return str(uri).strip(), ()
    encoded = row.get("base64_url")
    if _is_missing(encoded):
        return None, ("no uri and no base64_url",)
    decoded = _decode_base64_url(str(encoded).strip())
    if decoded is None:
        return None, ("uri was NULL and base64_url did not decode",)
    notes.append("url recovered from base64_url")
    return decoded, tuple(notes)


def resolve_feeds(names: Iterable[str], df: pd.DataFrame) -> list[FeedTarget]:
    """
    Turn warehouse rows into FeedTargets, one per requested name.

    Pure function over a DataFrame so it is testable without network access. Raises on names
    that returned no row - a silent drop would understate the search.
    """
    names = list(names)
    if df is None or df.empty:
        raise FeedResolutionError(f"no warehouse rows returned for any of: {names}")

    if "name" not in df.columns:
        raise FeedResolutionError(f"resolution query returned no 'name' column; got {list(df.columns)}")

    returned = set(df["name"].dropna().astype(str))
    missing = [n for n in names if n not in returned]
    if missing:
        raise FeedResolutionError(
            "no current vehicle_positions feed in the warehouse for: "
            + ", ".join(repr(n) for n in missing)
            + ". Check the exact name in dim_gtfs_datasets (names drift as feeds migrate vendors)."
        )

    targets: list[FeedTarget] = []
    seen_urls: dict[str, str] = {}

    for name in names:
        rows = df[df["name"].astype(str) == name]

        if len(rows) > 1:
            keys = sorted(str(k) for k in rows.get("key", pd.Series(dtype=str)).dropna())
            targets.append(
                FeedTarget(
                    feed_name=name,
                    status=STATUS_AMBIGUOUS_NAME,
                    notes=(f"{len(rows)} current rows share this name; keys: {', '.join(keys)}",),
                )
            )
            continue

        row = rows.iloc[0]
        notes: list[str] = []

        org_names_raw = row.get("organization_names")
        if isinstance(org_names_raw, (list, tuple)):
            organization_names = tuple(str(o) for o in org_names_raw)
        elif _is_missing(org_names_raw):
            organization_names = ()
        else:
            organization_names = (str(org_names_raw),)

        n_orgs_raw = row.get("n_organizations")
        n_organizations = 0 if _is_missing(n_orgs_raw) else int(n_orgs_raw)

        regional_feed_type = None if _is_missing(row.get("regional_feed_type")) else str(row.get("regional_feed_type"))
        # A combined-regional feed reports many agencies at once; flagging it stops anyone
        # reading the result as a single operator's cadence.
        is_regional = n_organizations > 1 or regional_feed_type is not None

        private_dataset = _is_true(row.get("private_dataset"))
        deprecated_raw = row.get("deprecated_date")
        deprecated_date = None if _is_missing(deprecated_raw) else str(deprecated_raw)
        if private_dataset:
            notes.append("private_dataset is set")
        if deprecated_date:
            notes.append(f"deprecated_date is set ({deprecated_date})")

        common = dict(
            feed_name=name,
            gtfs_dataset_key=None if _is_missing(row.get("key")) else str(row.get("key")),
            is_regional=is_regional,
            organization_names=organization_names,
            n_organizations=n_organizations,
            regional_feed_type=regional_feed_type,
            private_dataset=private_dataset,
            deprecated_date=deprecated_date,
        )

        if _is_true(row.get("has_authentication")):
            targets.append(
                FeedTarget(status=STATUS_SKIPPED_AUTH, notes=tuple(notes + ["has_authentication is set"]), **common)
            )
            continue

        url, url_notes = _resolve_url(row)
        notes.extend(url_notes)

        if url is None:
            targets.append(FeedTarget(status=STATUS_NO_URL, notes=tuple(notes), **common))
            continue

        # Some warehouse URIs are templates holding a secret placeholder, e.g.
        # "...?api_key={{ MTC_511_API_KEY}}". Fetching that literally would be meaningless.
        if "{{" in url:
            targets.append(
                FeedTarget(
                    status=STATUS_UNRESOLVED_URL_TEMPLATE,
                    url=url,
                    notes=tuple(notes + ["url still contains an unsubstituted {{ placeholder }}"]),
                    **common,
                )
            )
            continue

        if url in seen_urls:
            targets.append(
                FeedTarget(
                    status=STATUS_OK,
                    url=url,
                    duplicate_of=seen_urls[url],
                    notes=tuple(notes + [f"same URL as {seen_urls[url]!r}; fetched once"]),
                    **common,
                )
            )
            continue

        seen_urls[url] = name
        targets.append(FeedTarget(status=STATUS_OK, url=url, notes=tuple(notes), **common))

    return targets


def pollable_targets(targets: Iterable[FeedTarget]) -> list[FeedTarget]:
    """Targets to actually fetch: pollable, and only the first of each shared URL."""
    return [t for t in targets if t.is_pollable and t.duplicate_of is None]
