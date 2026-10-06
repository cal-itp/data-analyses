"""
Turn raw vehicle-position samples into per-feed update-cadence metrics.

The measurement: for each vehicle, when its position moves at least k metres between two
consecutive updates, the gap between those updates must be at most n seconds. A feed is
"frequent" when at least min_pass_fraction of those movement gaps pass.

Every exclusion is counted and reported. A gap dropped without a count would quietly inflate
the pass fraction, which is the whole number this tool exists to produce.
"""

from __future__ import annotations

from typing import Any, Optional

import numpy as np
import pandas as pd
from config import CriteriaConfig, SearchConfig
from feeds import STATUS_OK, FeedTarget

EARTH_RADIUS_M = 6_371_008.8

# A source timestamp this far beyond the moment we asked for it means a broken publisher clock,
# not a fresh position.
CLOCK_SKEW_TOLERANCE_SECONDS = 60.0

# Observed gaps cannot be finer than the poll interval. At or below this ratio we did not poll
# fast enough to resolve the feed's real cadence, whatever the pass fraction says.
RESOLUTION_LIMITED_RATIO = 1.5

# Vehicle ids churning this much faster than the per-poll fleet size means ids are unstable, so
# every update looks like a first sighting and gap counts collapse.
UNSTABLE_VEHICLE_ID_RATIO = 3.0

TIMESTAMP_SOURCE_VEHICLE = "vehicle"
TIMESTAMP_SOURCE_HEADER = "header"
TIMESTAMP_SOURCE_WALL_CLOCK = "wall_clock"

STATUS_DEGRADED_TIMESTAMPS = "degraded_timestamps"
STATUS_NO_DATA = "no_data"

GAP_COLUMNS = [
    "feed_name",
    "vehicle_id",
    "prev_poll_seq",
    "poll_seq",
    "prev_effective_timestamp",
    "effective_timestamp",
    "requested_at",
    "gap_seconds",
    "distance_m",
    "is_movement",
    "passes",
    "straddles_failed_poll",
    "straddles_absence",
    "is_stale",
    "is_invalid",
    "excluded",
    "timestamp_source",
]


def haversine_m(
    lat1: np.ndarray | float,
    lon1: np.ndarray | float,
    lat2: np.ndarray | float,
    lon2: np.ndarray | float,
) -> np.ndarray:
    """
    Great-circle distance in metres. Vectorized float64; no geopandas or pyproj.

    Positions arrive as WGS84 degrees straight off the protobuf, and we only need a movement
    threshold of a few metres, so a spherical approximation is plenty.
    """
    lat1 = np.asarray(lat1, dtype="float64")
    lon1 = np.asarray(lon1, dtype="float64")
    lat2 = np.asarray(lat2, dtype="float64")
    lon2 = np.asarray(lon2, dtype="float64")

    phi1, phi2 = np.radians(lat1), np.radians(lat2)
    dphi = phi2 - phi1
    dlambda = np.radians(lon2 - lon1)

    a = np.sin(dphi / 2.0) ** 2 + np.cos(phi1) * np.cos(phi2) * np.sin(dlambda / 2.0) ** 2
    return 2.0 * EARTH_RADIUS_M * np.arcsin(np.sqrt(np.clip(a, 0.0, 1.0)))


def assign_effective_timestamp(samples: pd.DataFrame) -> pd.DataFrame:
    """
    Pick the clock each sample is measured on: vehicle -> header -> poll wall-clock.

    The source is recorded per row because a wall-clock fallback makes every gap land at
    roughly the poll interval, which would look like a very fresh feed. summarize_feed refuses
    to call such a feed frequent.
    """
    df = samples.copy()

    vehicle_ts = pd.to_numeric(df.get("vehicle_timestamp"), errors="coerce")
    header_ts = pd.to_numeric(df.get("header_timestamp"), errors="coerce")
    requested = pd.to_numeric(df.get("requested_at"), errors="coerce")

    # Zero is how feeds spell "unset" for a protobuf uint64, so treat it as missing.
    vehicle_ts = vehicle_ts.where(vehicle_ts > 0)
    header_ts = header_ts.where(header_ts > 0)

    effective = vehicle_ts.copy()
    source = pd.Series(TIMESTAMP_SOURCE_VEHICLE, index=df.index, dtype="object")

    use_header = effective.isna() & header_ts.notna()
    effective = effective.where(~use_header, header_ts)
    source = source.where(~use_header, TIMESTAMP_SOURCE_HEADER)

    use_wall = effective.isna()
    effective = effective.where(~use_wall, requested)
    source = source.where(~use_wall, TIMESTAMP_SOURCE_WALL_CLOCK)

    df["effective_timestamp"] = effective.astype("float64")
    df["timestamp_source"] = source
    return df


def dedupe_within_poll(samples: pd.DataFrame) -> pd.DataFrame:
    """Keep the latest effective_timestamp per (feed, poll, vehicle) - some feeds emit a vehicle twice."""
    if samples.empty:
        return samples
    return (
        samples.sort_values(["feed_name", "poll_seq", "vehicle_id", "effective_timestamp"])
        .drop_duplicates(subset=["feed_name", "poll_seq", "vehicle_id"], keep="last")
        .reset_index(drop=True)
    )


def _failed_poll_seqs(poll_log: Optional[pd.DataFrame], feed_name: str) -> np.ndarray:
    """Sorted poll_seqs where this feed's fetch failed or was skipped."""
    if poll_log is None or poll_log.empty or "error_type" not in poll_log.columns:
        return np.empty(0, dtype="int64")
    rows = poll_log[(poll_log["feed_name"] == feed_name) & (poll_log["error_type"] != "ok")]
    if rows.empty:
        return np.empty(0, dtype="int64")
    return np.sort(rows["poll_seq"].astype("int64").to_numpy())


def compute_gaps(
    samples: pd.DataFrame,
    poll_log: Optional[pd.DataFrame] = None,
    criteria: Optional[CriteriaConfig] = None,
) -> pd.DataFrame:
    """
    One row per consecutive-update pair per vehicle, with every exclusion reason flagged.

    Expects samples that already hold only timestamp *changes* (the poller dedupes as it goes)
    plus a had_absence_since_last_change flag, which is the only place the information about a
    vehicle vanishing from the feed survives that dedupe.

    Pairs are formed in poll order, so a feed publishing a timestamp that moves backwards yields
    a negative gap and is flagged invalid rather than being quietly sorted into shape.
    """
    if criteria is None:
        criteria = CriteriaConfig(movement_threshold_meters=10.0, max_update_gap_seconds=20.0)

    if samples is None or samples.empty:
        return pd.DataFrame(columns=GAP_COLUMNS)

    df = samples if "effective_timestamp" in samples.columns else assign_effective_timestamp(samples)
    df = dedupe_within_poll(df)
    df = df.dropna(subset=["effective_timestamp", "vehicle_id"])
    if df.empty:
        return pd.DataFrame(columns=GAP_COLUMNS)

    # Sort by observation order, not by timestamp: the poller records only changes, so poll_seq
    # is the true arrival order. Sorting by effective_timestamp would silently reorder a feed's
    # backwards timestamp into a plausible positive gap, and is_invalid could never fire.
    df = df.sort_values(["feed_name", "vehicle_id", "poll_seq"]).reset_index(drop=True)
    grouped = df.groupby(["feed_name", "vehicle_id"], sort=False)

    gaps = pd.DataFrame(
        {
            "feed_name": df["feed_name"],
            "vehicle_id": df["vehicle_id"],
            "prev_poll_seq": grouped["poll_seq"].shift(1),
            "poll_seq": df["poll_seq"],
            "prev_effective_timestamp": grouped["effective_timestamp"].shift(1),
            "effective_timestamp": df["effective_timestamp"],
            "requested_at": pd.to_numeric(df.get("requested_at"), errors="coerce"),
            "gap_seconds": grouped["effective_timestamp"].diff(),
            "prev_latitude": grouped["latitude"].shift(1),
            "prev_longitude": grouped["longitude"].shift(1),
            "latitude": pd.to_numeric(df["latitude"], errors="coerce"),
            "longitude": pd.to_numeric(df["longitude"], errors="coerce"),
            "timestamp_source": df["timestamp_source"],
            # Recorded on the later sample of the pair, which is the row the gap sits on.
            "straddles_absence": (
                df["had_absence_since_last_change"].fillna(False).astype(bool)
                if "had_absence_since_last_change" in df.columns
                else False
            ),
        }
    )
    # The first sighting of each vehicle is not a gap.
    gaps = gaps.dropna(subset=["gap_seconds"]).reset_index(drop=True)
    if gaps.empty:
        return pd.DataFrame(columns=GAP_COLUMNS)

    gaps["distance_m"] = haversine_m(
        gaps["prev_latitude"].to_numpy(),
        gaps["prev_longitude"].to_numpy(),
        gaps["latitude"].to_numpy(),
        gaps["longitude"].to_numpy(),
    )

    gaps["is_movement"] = gaps["distance_m"] >= criteria.movement_threshold_meters
    gaps["passes"] = gaps["gap_seconds"] <= criteria.max_update_gap_seconds
    gaps["is_stale"] = gaps["gap_seconds"] > criteria.stale_gap_seconds

    # A non-monotonic gap, or a source clock running ahead of the moment we asked, is not a
    # measurement of anything.
    future_timestamp = gaps["effective_timestamp"] > gaps["requested_at"] + CLOCK_SKEW_TOLERANCE_SECONDS
    gaps["is_invalid"] = (gaps["gap_seconds"] <= 0) | future_timestamp.fillna(False)

    straddles_failed = np.zeros(len(gaps), dtype=bool)
    for feed_name, idx in gaps.groupby("feed_name", sort=False).groups.items():
        failed = _failed_poll_seqs(poll_log, feed_name)
        if failed.size == 0:
            continue
        positions = gaps.index.get_indexer(idx)
        prev_seq = gaps.loc[idx, "prev_poll_seq"].to_numpy(dtype="float64")
        curr_seq = gaps.loc[idx, "poll_seq"].to_numpy(dtype="float64")
        # Count failed ticks strictly between the two polls the samples came from.
        n_failed_between = np.searchsorted(failed, curr_seq, side="left") - np.searchsorted(
            failed, prev_seq, side="right"
        )
        straddles_failed[positions] = n_failed_between > 0
    gaps["straddles_failed_poll"] = straddles_failed

    excluded = gaps["is_invalid"] | gaps["is_stale"]
    if criteria.exclude_absence_gaps:
        excluded = excluded | gaps["straddles_failed_poll"] | gaps["straddles_absence"]
    gaps["excluded"] = excluded

    return gaps[GAP_COLUMNS].reset_index(drop=True)


def _percentiles(values: pd.Series, percentiles: tuple[int, ...], prefix: str) -> dict[str, Optional[float]]:
    clean = pd.to_numeric(values, errors="coerce").dropna()
    out: dict[str, Optional[float]] = {}
    for p in percentiles:
        out[f"{prefix}_p{p}"] = float(np.percentile(clean, p)) if len(clean) else None
    return out


def _mode_or_none(values: pd.Series) -> Optional[str]:
    clean = values.dropna()
    if clean.empty:
        return None
    return str(clean.mode().iloc[0])


def summarize_feed(
    feed_name: str,
    gaps: pd.DataFrame,
    poll_log: pd.DataFrame,
    config: SearchConfig,
    target: Optional[FeedTarget] = None,
    config_warnings: Optional[list[str]] = None,
) -> dict[str, Any]:
    """
    One summary row for one feed.

    Three honesty guards sit at the end, because each can otherwise manufacture a false
    "frequent": a wall-clock timestamp fallback, polling too slowly to resolve the real cadence,
    and churning vehicle ids.
    """
    criteria = config.criteria
    percentiles = tuple(config.output.percentiles)

    feed_log = poll_log[poll_log["feed_name"] == feed_name] if poll_log is not None and not poll_log.empty else None
    feed_gaps = gaps[gaps["feed_name"] == feed_name] if gaps is not None and not gaps.empty else None

    n_polls_attempted = 0 if feed_log is None else int(len(feed_log))
    ok_polls = None if feed_log is None else feed_log[feed_log["error_type"] == "ok"]
    n_polls_ok = 0 if ok_polls is None else int(len(ok_polls))
    n_polls_failed = n_polls_attempted - n_polls_ok

    # Fleet size per poll comes from the poll log's entity count, not from the samples: samples
    # hold only timestamp changes, so they cannot say how many vehicles a poll actually carried.
    median_vehicles_per_poll = None
    if ok_polls is not None and not ok_polls.empty and "n_entities" in ok_polls.columns:
        entity_counts = pd.to_numeric(ok_polls["n_entities"], errors="coerce").dropna()
        if not entity_counts.empty:
            median_vehicles_per_poll = float(entity_counts.median())

    summary: dict[str, Any] = {
        "feed_name": feed_name,
        "status": target.status if target is not None else STATUS_OK,
        "url": target.url if target is not None else None,
        "n_polls_attempted": n_polls_attempted,
        "n_polls_ok": n_polls_ok,
        "n_polls_failed": n_polls_failed,
        "median_vehicles_per_poll": median_vehicles_per_poll,
        "is_regional": bool(target.is_regional) if target is not None else False,
        "organization_names": ", ".join(target.organization_names) if target is not None else "",
        "regional_feed_type": target.regional_feed_type if target is not None else None,
        "poll_interval_warning": "; ".join(config_warnings or []) or None,
        "notes": "; ".join(target.notes) if target is not None and target.notes else None,
    }

    error_summary = None
    if feed_log is not None and not feed_log.empty:
        errors = feed_log.loc[feed_log["error_type"] != "ok", "error_type"].value_counts()
        if not errors.empty:
            error_summary = ", ".join(f"{k}={v}" for k, v in errors.items())
    summary["error_summary"] = error_summary

    empty_metrics = {
        "n_distinct_vehicles": 0,
        "n_updates": 0,
        "n_update_gaps": 0,
        "n_movement_gaps": 0,
        "pass_fraction": None,
        "is_frequent": None,
        "n_excluded_stale": 0,
        "n_excluded_absence": 0,
        "n_excluded_failed_poll": 0,
        "n_invalid_gaps": 0,
        "timestamp_source_mode": None,
        "vehicle_id_source_mode": None,
        "quantization_ratio": None,
        "resolution_limited": False,
        "unstable_vehicle_ids": False,
    }

    if feed_gaps is None or feed_gaps.empty:
        summary.update(empty_metrics)
        summary.update(_percentiles(pd.Series(dtype="float64"), percentiles, "update_gap"))
        summary.update(_percentiles(pd.Series(dtype="float64"), percentiles, "movement_gap"))
        if summary["status"] == STATUS_OK:
            summary["status"] = STATUS_NO_DATA
        return summary

    summary["n_distinct_vehicles"] = int(feed_gaps["vehicle_id"].nunique())
    summary["n_updates"] = int(len(feed_gaps)) + summary["n_distinct_vehicles"]
    summary["n_invalid_gaps"] = int(feed_gaps["is_invalid"].sum())
    summary["n_excluded_stale"] = int((feed_gaps["is_stale"] & ~feed_gaps["is_invalid"]).sum())
    summary["n_excluded_failed_poll"] = int(feed_gaps["straddles_failed_poll"].sum())
    summary["n_excluded_absence"] = int(feed_gaps["straddles_absence"].sum())
    summary["timestamp_source_mode"] = _mode_or_none(feed_gaps["timestamp_source"])

    usable = feed_gaps[~feed_gaps["excluded"]]
    summary["n_update_gaps"] = int(len(usable))
    summary.update(_percentiles(usable["gap_seconds"], percentiles, "update_gap"))

    movement = usable[usable["is_movement"]]
    summary["n_movement_gaps"] = int(len(movement))
    summary.update(_percentiles(movement["gap_seconds"], percentiles, "movement_gap"))

    pass_fraction = float(movement["passes"].mean()) if len(movement) else None
    summary["pass_fraction"] = pass_fraction
    summary["is_frequent"] = None if pass_fraction is None else bool(pass_fraction >= criteria.min_pass_fraction)

    update_p50 = float(np.percentile(usable["gap_seconds"], 50)) if len(usable) else None
    interval = config.sampling.poll_interval_seconds
    quantization_ratio = (update_p50 / interval) if (update_p50 is not None and interval > 0) else None
    summary["quantization_ratio"] = quantization_ratio

    # Guard 1: without vehicle timestamps every gap collapses to the poll interval, so the
    # pass fraction measures our own cadence rather than the feed's.
    if summary["timestamp_source_mode"] != TIMESTAMP_SOURCE_VEHICLE:
        summary["status"] = STATUS_DEGRADED_TIMESTAMPS
        summary["is_frequent"] = None

    # Guard 2: we cannot observe a gap finer than we polled.
    summary["resolution_limited"] = bool(
        quantization_ratio is not None and quantization_ratio <= RESOLUTION_LIMITED_RATIO
    )

    # Guard 3: churning ids make every update look like a first sighting.
    summary["unstable_vehicle_ids"] = bool(
        median_vehicles_per_poll
        and median_vehicles_per_poll > 0
        and summary["n_distinct_vehicles"] > UNSTABLE_VEHICLE_ID_RATIO * median_vehicles_per_poll
    )

    summary["vehicle_id_source_mode"] = None
    return summary


def summarize_run(
    samples: pd.DataFrame,
    poll_log: pd.DataFrame,
    config: SearchConfig,
    targets: list[FeedTarget],
    config_warnings: Optional[list[str]] = None,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Compute gaps once, then one summary row per requested feed. Returns (summary, gaps)."""
    gaps = compute_gaps(samples, poll_log, config.criteria)

    id_source_mode = {}
    if samples is not None and not samples.empty and "vehicle_id_source" in samples.columns:
        for feed_name, rows in samples.groupby("feed_name", sort=False):
            id_source_mode[feed_name] = _mode_or_none(rows["vehicle_id_source"])

    rows = []
    for target in targets:
        row = summarize_feed(target.feed_name, gaps, poll_log, config, target, config_warnings)
        row["vehicle_id_source_mode"] = id_source_mode.get(target.feed_name)
        rows.append(row)

    return pd.DataFrame(rows), gaps
