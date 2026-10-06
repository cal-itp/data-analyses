import base64

import pandas as pd
import pytest
from conftest import FIXTURES_DIR
from feeds import (
    STATUS_AMBIGUOUS_NAME,
    STATUS_NO_URL,
    STATUS_OK,
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
