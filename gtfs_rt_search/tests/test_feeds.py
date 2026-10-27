import base64

import pandas as pd
import pytest
from conftest import FIXTURES_DIR
from feeds import (
    STATUS_AMBIGUOUS_NAME,
    STATUS_NO_URL,
    STATUS_OK,
    STATUS_SECRET_UNAVAILABLE,
    STATUS_SKIPPED_AUTH,
    STATUS_UNRESOLVED_URL_TEMPLATE,
    FeedResolutionError,
    _is_true,
    build_query,
    pollable_targets,
    resolve_feeds,
)

SAMPLE_CSV = FIXTURES_DIR / "dim_gtfs_datasets_sample.csv"


@pytest.fixture
def warehouse():
    """Current vehicle_positions rows, as the resolution query would return them."""
    df = pd.read_csv(SAMPLE_CSV)
    return df[df["_is_current"] == True].reset_index(drop=True)  # noqa: E712


def only(targets, name):
    return next(t for t in targets if t.feed_name == name)


# -- the NaN trap ---------------------------------------------------------------------
# has_authentication is never False in the warehouse: it is True or NULL. bool(nan) is True
# in Python, so a plain truthiness test would skip every single feed.


def test_is_true_treats_nan_as_false():
    assert _is_true(float("nan")) is False
    assert _is_true(None) is False
    assert _is_true("") is False
    assert _is_true(True) is True
    assert _is_true("true") is True


def test_nan_authentication_does_not_skip_feed(warehouse):
    assert warehouse.loc[warehouse["name"] == "VCTC Vehicle Positions", "has_authentication"].isna().all()
    target = only(resolve_feeds(["VCTC Vehicle Positions"], warehouse), "VCTC Vehicle Positions")
    assert target.status == STATUS_OK
    assert target.is_pollable


# -- resolution paths -----------------------------------------------------------------


def test_clean_regional_feed(warehouse):
    target = only(resolve_feeds(["VCTC Vehicle Positions"], warehouse), "VCTC Vehicle Positions")
    assert target.url == "https://govcbus.com/gtfs-rt/vehiclepositions"
    # A Combined Regional Feed reports many agencies, so it must not read as one operator.
    assert target.regional_feed_type == "Combined Regional Feed"
    assert target.is_regional is True


def test_single_operator_feed_is_not_regional(warehouse):
    target = only(resolve_feeds(["Long Beach VehiclePositions"], warehouse), "Long Beach VehiclePositions")
    assert target.status == STATUS_OK
    assert target.is_regional is False


def test_authenticated_feed_is_skipped(warehouse):
    name = "Big Blue Bus Swiftly Vehicle Positions"
    target = only(resolve_feeds([name], warehouse), name)
    assert target.status == STATUS_SKIPPED_AUTH
    assert not target.is_pollable
    assert any("has_authentication" in n for n in target.notes)
    # Also private and deprecated; both are recorded rather than being the reason for the skip.
    assert target.private_dataset is True
    assert any("deprecated_date" in n for n in target.notes)


def test_name_with_no_current_row_is_a_hard_error(warehouse):
    # Present in the warehouse, but only as a superseded row - the feed migrated to TripShot.
    with pytest.raises(FeedResolutionError, match="Emery Go-Round VehiclePositions"):
        resolve_feeds(["Emery Go-Round VehiclePositions"], warehouse)


def test_hard_error_lists_every_missing_name(warehouse):
    with pytest.raises(FeedResolutionError) as exc:
        resolve_feeds(["VCTC Vehicle Positions", "Totally Made Up Feed"], warehouse)
    assert "Totally Made Up Feed" in str(exc.value)


def test_duplicate_name_is_ambiguous(warehouse):
    row = warehouse[warehouse["name"] == "VCTC Vehicle Positions"]
    doubled = pd.concat([warehouse, row.assign(key="a-different-key")], ignore_index=True)
    target = only(resolve_feeds(["VCTC Vehicle Positions"], doubled), "VCTC Vehicle Positions")
    assert target.status == STATUS_AMBIGUOUS_NAME
    assert "a-different-key" in target.notes[0]


def test_unsubstituted_url_template_is_skipped():
    # All such feeds in the warehouse today also carry has_authentication, so this guard is
    # defence in depth rather than a path real data reaches.
    df = pd.DataFrame(
        [
            {
                "key": "k1",
                "name": "Templated Feed",
                "uri": "https://api.511.org/transit/vehiclepositions?api_key={{ MTC_511_API_KEY}}&agency=EM",
                "base64_url": None,
                "has_authentication": None,
                "private_dataset": None,
                "deprecated_date": None,
                "regional_feed_type": None,
                "n_organizations": 1,
            }
        ]
    )
    target = only(resolve_feeds(["Templated Feed"], df), "Templated Feed")
    assert target.status == STATUS_UNRESOLVED_URL_TEMPLATE
    assert not target.is_pollable


def test_falls_back_to_base64_url_when_uri_is_null():
    url = "https://example.test/vp"
    df = pd.DataFrame(
        [
            {
                "key": "k1",
                "name": "Base64 Only",
                "uri": None,
                "base64_url": base64.urlsafe_b64encode(url.encode()).decode().rstrip("="),
                "has_authentication": None,
                "n_organizations": 1,
            }
        ]
    )
    target = only(resolve_feeds(["Base64 Only"], df), "Base64 Only")
    assert target.url == url
    assert any("base64_url" in n for n in target.notes)


def test_no_url_at_all():
    df = pd.DataFrame([{"key": "k1", "name": "No URL", "uri": None, "base64_url": None}])
    assert only(resolve_feeds(["No URL"], df), "No URL").status == STATUS_NO_URL


def test_shared_url_is_fetched_once(warehouse):
    shared = warehouse[warehouse["name"] == "VCTC Vehicle Positions"].iloc[0]
    twin = shared.copy()
    twin["name"] = "VCTC Mirror"
    twin["key"] = "mirror-key"
    df = pd.concat([warehouse, pd.DataFrame([twin])], ignore_index=True)

    targets = resolve_feeds(["VCTC Vehicle Positions", "VCTC Mirror"], df)
    assert only(targets, "VCTC Mirror").duplicate_of == "VCTC Vehicle Positions"
    assert [t.feed_name for t in pollable_targets(targets)] == ["VCTC Vehicle Positions"]


def test_resolution_preserves_requested_order(warehouse):
    names = ["Long Beach VehiclePositions", "VCTC Vehicle Positions"]
    assert [t.feed_name for t in resolve_feeds(names, warehouse)] == names


# -- query construction ---------------------------------------------------------------


def test_query_interpolates_names_rather_than_leaving_a_jinja_placeholder():
    sql = build_query(["VCTC Vehicle Positions", "Long Beach VehiclePositions"])
    assert "'VCTC Vehicle Positions', 'Long Beach VehiclePositions'" in sql
    # query_sql renders SQL through Jinja with only USER_DEFINED_MACROS in context, so a
    # surviving placeholder would render empty and produce IN ().
    assert "{{" not in sql
    assert "IN ()" not in sql


def test_query_rejects_quote_injection():
    with pytest.raises(FeedResolutionError, match="unsupported character"):
        build_query(["Feed' OR 1=1 --"])


def test_query_preaggregates_provider_table():
    sql = build_query(["VCTC Vehicle Positions"])
    # Pre-aggregating in a CTE is what prevents the many-to-many fanout.
    assert "prov AS (" in sql and "GROUP BY 1" in sql


# -- credential injection from Secret Manager ------------------------------------------


class StubResolver:
    """Substitutes {{ NAME }} from a dict, or raises, without touching Secret Manager."""

    def __init__(self, values=None, error=None):
        self.values = values or {}
        self.error = error
        self.calls = 0

    def resolve(self, url):
        from auth import PLACEHOLDER_RE, SecretResolutionError, register_secret_value

        self.calls += 1
        if self.error is not None:
            raise SecretResolutionError(self.error)

        def sub(match):
            name = match.group(1)
            if name not in self.values:
                raise SecretResolutionError(f"could not read secret {name!r}")
            register_secret_value(self.values[name])
            return self.values[name]

        return PLACEHOLDER_RE.sub(sub, url)


@pytest.fixture(autouse=True)
def _clean_secret_registry():
    from auth import clear_registered_secrets

    clear_registered_secrets()
    yield
    clear_registered_secrets()


def templated_warehouse_row(has_authentication=True):
    return pd.DataFrame(
        [
            {
                "key": "k1",
                "name": "Bay Area 511 Emery Go-Round VehiclePositions",
                "uri": "https://api.511.org/transit/vehiclepositions?api_key={{ MTC_511_API_KEY}}&agency=EM",
                "base64_url": None,
                "has_authentication": has_authentication,
                "private_dataset": None,
                "deprecated_date": None,
                "regional_feed_type": None,
                "n_organizations": 1,
            }
        ]
    )


FEED_511 = "Bay Area 511 Emery Go-Round VehiclePositions"


def test_authenticated_feed_becomes_pollable_once_its_secret_is_injected():
    """
    The behavioural change: has_authentication alone no longer means skip. A feed whose URL
    carries a {{ }} placeholder we can fill is pollable.
    """
    resolver = StubResolver({"MTC_511_API_KEY": "real-key-123456"})
    target = only(resolve_feeds([FEED_511], templated_warehouse_row(), secret_resolver=resolver), FEED_511)

    assert target.status == STATUS_OK
    assert target.is_pollable
    assert target.url == "https://api.511.org/transit/vehiclepositions?api_key=real-key-123456&agency=EM"
    assert target.secret_names == ("MTC_511_API_KEY",)
    assert any("credential injected" in n for n in target.notes)


def test_safe_url_masks_the_injected_credential():
    resolver = StubResolver({"MTC_511_API_KEY": "real-key-123456"})
    target = only(resolve_feeds([FEED_511], templated_warehouse_row(), secret_resolver=resolver), FEED_511)

    assert "real-key-123456" in target.url  # the poller needs the real thing
    assert "real-key-123456" not in target.safe_url  # nothing else may see it
    assert "agency=EM" in target.safe_url


def test_without_a_resolver_a_templated_url_is_left_unresolved():
    target = only(resolve_feeds([FEED_511], templated_warehouse_row()), FEED_511)
    assert target.status == STATUS_UNRESOLVED_URL_TEMPLATE
    assert not target.is_pollable
    assert target.secret_names == ("MTC_511_API_KEY",)
    assert any("no resolver was configured" in n for n in target.notes)


def test_unreadable_secret_reports_secret_unavailable():
    resolver = StubResolver(error="could not read secret 'MTC_511_API_KEY': PermissionDenied")
    target = only(resolve_feeds([FEED_511], templated_warehouse_row(), secret_resolver=resolver), FEED_511)

    assert target.status == STATUS_SECRET_UNAVAILABLE
    assert not target.is_pollable
    # Distinguishable from "we never tried" and from "authentication is impossible".
    assert target.status != STATUS_UNRESOLVED_URL_TEMPLATE
    assert target.status != STATUS_SKIPPED_AUTH
    assert any("PermissionDenied" in n for n in target.notes)


def test_authenticated_feed_with_no_placeholder_is_still_skipped():
    """Nothing to inject, so there is no way to authenticate: skip, as before."""
    df = pd.DataFrame(
        [
            {
                "key": "k1",
                "name": "Opaque Auth Feed",
                "uri": "https://api.example.org/vp",
                "base64_url": None,
                "has_authentication": True,
                "n_organizations": 1,
            }
        ]
    )
    target = only(resolve_feeds(["Opaque Auth Feed"], df, secret_resolver=StubResolver()), "Opaque Auth Feed")
    assert target.status == STATUS_SKIPPED_AUTH
    assert any("carries no {{ secret }}" in n for n in target.notes)


def test_unauthenticated_feed_with_a_placeholder_is_still_resolved():
    resolver = StubResolver({"MTC_511_API_KEY": "real-key-123456"})
    target = only(
        resolve_feeds([FEED_511], templated_warehouse_row(has_authentication=None), secret_resolver=resolver),
        FEED_511,
    )
    assert target.status == STATUS_OK


def test_resolver_is_not_called_for_feeds_without_placeholders(warehouse):
    resolver = StubResolver()
    resolve_feeds(["VCTC Vehicle Positions"], warehouse, secret_resolver=resolver)
    assert resolver.calls == 0


def test_two_feeds_sharing_one_key_resolve_to_distinct_urls():
    """Same secret, different agency: both pollable, neither deduped away."""
    base = templated_warehouse_row().iloc[0]
    second = base.copy()
    second["name"] = "Bay Area 511 Emery Express VehiclePositions"
    second["key"] = "k2"
    second["uri"] = base["uri"].replace("agency=EM", "agency=EE")
    df = pd.DataFrame([base, second])

    resolver = StubResolver({"MTC_511_API_KEY": "real-key-123456"})
    targets = resolve_feeds([FEED_511, "Bay Area 511 Emery Express VehiclePositions"], df, secret_resolver=resolver)

    assert [t.status for t in targets] == [STATUS_OK, STATUS_OK]
    assert len(pollable_targets(targets)) == 2
    assert all("real-key-123456" not in t.safe_url for t in targets)
