import numpy as np
import pandas as pd
import pytest
from config import CriteriaConfig, OutputConfig, SamplingConfig, SearchConfig
from feeds import FeedTarget
from metrics import (
    STATUS_DEGRADED_TIMESTAMPS,
    assign_effective_timestamp,
    compute_gaps,
    haversine_m,
    summarize_feed,
)

FEED = "Test Feed"
CRITERIA = CriteriaConfig(
    movement_threshold_meters=10.0,
    max_update_gap_seconds=20.0,
    min_pass_fraction=0.9,
    stale_gap_seconds=600.0,
    exclude_absence_gaps=True,
)


def make_config(poll_interval=5.0, criteria=CRITERIA):
    return SearchConfig(
        feeds=(FEED,),
        sampling=SamplingConfig(duration_minutes=30, poll_interval_seconds=poll_interval),
        criteria=criteria,
        output=OutputConfig(),
    )


def sample(poll_seq, ts, lat=34.0, lon=-118.0, vehicle_id="v1", absence=False, requested_at=None):
    return {
        "feed_name": FEED,
        "poll_seq": poll_seq,
        "scheduled_at": float(ts),
        "requested_at": float(ts if requested_at is None else requested_at),
        "vehicle_id": vehicle_id,
        "vehicle_id_source": "vehicle.id",
        "vehicle_timestamp": ts,
        "header_timestamp": ts,
        "latitude": lat,
        "longitude": lon,
        "bearing": None,
        "speed": None,
        "current_status": 2,
        "trip_id": "t1",
        "had_absence_since_last_change": absence,
    }


# -- haversine ------------------------------------------------------------------------


def test_haversine_known_distances():
    # One degree of latitude is ~111.2 km.
    assert haversine_m(0.0, 0.0, 1.0, 0.0) == pytest.approx(111_195, rel=1e-3)
    # LAX -> JFK, ~3,974 km.
    assert haversine_m(33.9425, -118.4081, 40.6413, -73.7781) == pytest.approx(3_974_200, rel=1e-3)


def test_haversine_zero_and_vectorized():
    assert haversine_m(34.0, -118.0, 34.0, -118.0) == pytest.approx(0.0, abs=1e-9)
    out = haversine_m(np.array([0.0, 0.0]), np.array([0.0, 0.0]), np.array([0.0, 1.0]), np.array([0.0, 0.0]))
    assert out.shape == (2,)
    assert out[0] == pytest.approx(0.0, abs=1e-9)


def test_haversine_small_movement_near_threshold():
    # ~0.0001 deg of latitude is ~11.1 m, just over the 10 m movement threshold.
    assert haversine_m(34.0, -118.0, 34.0001, -118.0) == pytest.approx(11.1, rel=0.05)


# -- effective timestamp --------------------------------------------------------------


def test_effective_timestamp_prefers_vehicle_then_header_then_wall_clock():
    df = pd.DataFrame(
        [
            {"vehicle_timestamp": 100, "header_timestamp": 200, "requested_at": 300.0},
            {"vehicle_timestamp": None, "header_timestamp": 200, "requested_at": 300.0},
            {"vehicle_timestamp": None, "header_timestamp": None, "requested_at": 300.0},
            {"vehicle_timestamp": 0, "header_timestamp": 0, "requested_at": 300.0},
        ]
    )
    out = assign_effective_timestamp(df)
    assert list(out["effective_timestamp"]) == [100.0, 200.0, 300.0, 300.0]
    assert list(out["timestamp_source"]) == ["vehicle", "header", "wall_clock", "wall_clock"]


# -- compute_gaps ---------------------------------------------------------------------


def test_moving_vehicle_produces_passing_gaps():
    samples = pd.DataFrame([sample(0, 1000), sample(1, 1010, lat=34.001)])
    gaps = compute_gaps(samples, pd.DataFrame(), CRITERIA)
    assert len(gaps) == 1
    row = gaps.iloc[0]
    assert row["gap_seconds"] == 10
    assert row["is_movement"]  # ~111 m
    assert row["passes"]
    assert not row["excluded"]


def test_stationary_vehicle_gap_is_not_a_movement_gap():
    samples = pd.DataFrame([sample(0, 1000), sample(1, 1005)])
    gaps = compute_gaps(samples, pd.DataFrame(), CRITERIA)
    assert not gaps.iloc[0]["is_movement"]
    assert not gaps.iloc[0]["excluded"]


def test_negative_gap_is_invalid_and_excluded():
    samples = pd.DataFrame([sample(0, 1000), sample(1, 1000)])
    # Same timestamp twice across two polls: the dedupe would normally stop this, so force it.
    samples.loc[1, "vehicle_timestamp"] = 990
    samples.loc[1, "requested_at"] = 1000.0
    gaps = compute_gaps(samples, pd.DataFrame(), CRITERIA)
    assert gaps.iloc[0]["is_invalid"]
    assert gaps.iloc[0]["excluded"]


def test_stale_gap_is_excluded_and_counted():
    samples = pd.DataFrame([sample(0, 1000), sample(200, 2000, lat=34.01)])
    gaps = compute_gaps(samples, pd.DataFrame(), CRITERIA)
    assert gaps.iloc[0]["gap_seconds"] == 1000
    assert gaps.iloc[0]["is_stale"]
    assert gaps.iloc[0]["excluded"]


def test_future_timestamp_is_invalid():
    # Publisher clock running two minutes ahead of when we asked.
    samples = pd.DataFrame([sample(0, 1000), sample(1, 1130, requested_at=1005)])
    gaps = compute_gaps(samples, pd.DataFrame(), CRITERIA)
    assert gaps.iloc[0]["is_invalid"]


def test_gap_straddling_a_failed_poll_is_excluded():
    samples = pd.DataFrame([sample(0, 1000), sample(2, 1010, lat=34.001)])
    poll_log = pd.DataFrame(
        [
            {"feed_name": FEED, "poll_seq": 0, "error_type": "ok", "n_entities": 1},
            {"feed_name": FEED, "poll_seq": 1, "error_type": "timeout", "n_entities": None},
            {"feed_name": FEED, "poll_seq": 2, "error_type": "ok", "n_entities": 1},
        ]
    )
    gaps = compute_gaps(samples, poll_log, CRITERIA)
    assert gaps.iloc[0]["straddles_failed_poll"]
    assert gaps.iloc[0]["excluded"]


def test_consecutive_polls_do_not_straddle_a_later_failure():
    samples = pd.DataFrame([sample(0, 1000), sample(1, 1010, lat=34.001)])
    poll_log = pd.DataFrame(
        [
            {"feed_name": FEED, "poll_seq": 0, "error_type": "ok", "n_entities": 1},
            {"feed_name": FEED, "poll_seq": 1, "error_type": "ok", "n_entities": 1},
            {"feed_name": FEED, "poll_seq": 2, "error_type": "timeout", "n_entities": None},
        ]
    )
    gaps = compute_gaps(samples, poll_log, CRITERIA)
    assert not gaps.iloc[0]["straddles_failed_poll"]
    assert not gaps.iloc[0]["excluded"]


def test_absence_flag_excludes_the_gap_it_sits_on():
    # The vehicle vanished from the feed between these two updates; the poller stamps the flag
    # on the later sample, which is the only place that survives the timestamp dedupe.
    samples = pd.DataFrame([sample(0, 1000), sample(5, 1010, lat=34.001, absence=True)])
    gaps = compute_gaps(samples, pd.DataFrame(), CRITERIA)
    assert gaps.iloc[0]["straddles_absence"]
    assert gaps.iloc[0]["excluded"]


def test_absence_flag_is_kept_when_exclusion_is_disabled():
    criteria = CriteriaConfig(movement_threshold_meters=10.0, max_update_gap_seconds=20.0, exclude_absence_gaps=False)
    samples = pd.DataFrame([sample(0, 1000), sample(5, 1010, lat=34.001, absence=True)])
    gaps = compute_gaps(samples, pd.DataFrame(), criteria)
    assert gaps.iloc[0]["straddles_absence"]
    assert not gaps.iloc[0]["excluded"]


def test_absence_flag_lands_on_the_right_vehicle():
    # Two vehicles interleaved; only v2's second update follows an absence.
    samples = pd.DataFrame(
        [
            sample(0, 1000, vehicle_id="v1"),
            sample(0, 1000, vehicle_id="v2"),
            sample(5, 1010, vehicle_id="v1", lat=34.001),
            sample(5, 1010, vehicle_id="v2", lat=34.001, absence=True),
        ]
    )
    gaps = compute_gaps(samples, pd.DataFrame(), CRITERIA).set_index("vehicle_id")
    assert not gaps.loc["v1", "straddles_absence"]
    assert gaps.loc["v2", "straddles_absence"]


def test_duplicate_vehicle_within_one_poll_keeps_latest():
    samples = pd.DataFrame([sample(0, 1000), sample(0, 1005), sample(1, 1010, lat=34.001)])
    gaps = compute_gaps(samples, pd.DataFrame(), CRITERIA)
    assert len(gaps) == 1
    assert gaps.iloc[0]["gap_seconds"] == 5


def test_first_sighting_is_not_a_gap():
    gaps = compute_gaps(pd.DataFrame([sample(0, 1000)]), pd.DataFrame(), CRITERIA)
    assert gaps.empty


def test_empty_input():
    assert compute_gaps(pd.DataFrame(), pd.DataFrame(), CRITERIA).empty


# -- boundaries ------------------------------------------------------------------------


def test_gap_exactly_n_passes():
    samples = pd.DataFrame([sample(0, 1000), sample(1, 1020, lat=34.001)])
    gaps = compute_gaps(samples, pd.DataFrame(), CRITERIA)
    assert gaps.iloc[0]["gap_seconds"] == 20
    assert gaps.iloc[0]["passes"]


def test_gap_one_second_over_n_fails():
    samples = pd.DataFrame([sample(0, 1000), sample(1, 1021, lat=34.001)])
    assert not compute_gaps(samples, pd.DataFrame(), CRITERIA).iloc[0]["passes"]


def test_movement_exactly_k_counts():
    # 0.00009 deg latitude is ~10.0 m; is_movement uses >=, so it must count.
    criteria = CriteriaConfig(movement_threshold_meters=10.0, max_update_gap_seconds=20.0)
    distance = haversine_m(34.0, -118.0, 34.00009, -118.0)
    assert distance >= 10.0
    samples = pd.DataFrame([sample(0, 1000), sample(1, 1010, lat=34.00009)])
    assert compute_gaps(samples, pd.DataFrame(), criteria).iloc[0]["is_movement"]


def test_movement_below_k_does_not_count():
    samples = pd.DataFrame([sample(0, 1000), sample(1, 1010, lat=34.00001)])  # ~1.1 m
    assert not compute_gaps(samples, pd.DataFrame(), CRITERIA).iloc[0]["is_movement"]


# -- summarize_feed --------------------------------------------------------------------


def ok_log(n_polls, n_entities=1):
    return pd.DataFrame(
        [{"feed_name": FEED, "poll_seq": i, "error_type": "ok", "n_entities": n_entities} for i in range(n_polls)]
    )


def test_pass_fraction_boundary_is_frequent():
    # 9 of 10 movement gaps pass, exactly min_pass_fraction.
    rows = [sample(0, 1000)]
    ts = 1000
    for i in range(9):
        ts += 20  # exactly n, passes
        rows.append(sample(i + 1, ts, lat=34.0 + 0.001 * (i + 1)))
    ts += 25  # over n, fails
    rows.append(sample(10, ts, lat=34.02))

    gaps = compute_gaps(pd.DataFrame(rows), ok_log(11), CRITERIA)
    summary = summarize_feed(FEED, gaps, ok_log(11), make_config(), FeedTarget(FEED, "ok", url="u"))
    assert summary["n_movement_gaps"] == 10
    assert summary["pass_fraction"] == pytest.approx(0.9)
    assert summary["is_frequent"] is True


def test_pass_fraction_just_below_boundary_is_not_frequent():
    rows = [sample(0, 1000)]
    ts = 1000
    for i in range(8):
        ts += 20
        rows.append(sample(i + 1, ts, lat=34.0 + 0.001 * (i + 1)))
    for i in range(2):
        ts += 25
        rows.append(sample(9 + i, ts, lat=34.01 + 0.001 * i))

    gaps = compute_gaps(pd.DataFrame(rows), ok_log(11), CRITERIA)
    summary = summarize_feed(FEED, gaps, ok_log(11), make_config(), FeedTarget(FEED, "ok", url="u"))
    assert summary["pass_fraction"] == pytest.approx(0.8)
    assert summary["is_frequent"] is False


def test_exclusions_are_counted_not_silently_dropped():
    rows = [
        sample(0, 1000),
        sample(1, 1010, lat=34.001),  # good
        sample(2, 2000, lat=34.01),  # stale
        sample(5, 2010, lat=34.02, absence=True),  # absence
    ]
    poll_log = pd.DataFrame(
        [
            {"feed_name": FEED, "poll_seq": i, "error_type": "ok" if i != 3 else "timeout", "n_entities": 1}
            for i in range(6)
        ]
    )
    gaps = compute_gaps(pd.DataFrame(rows), poll_log, CRITERIA)
    summary = summarize_feed(FEED, gaps, poll_log, make_config(), FeedTarget(FEED, "ok", url="u"))
    assert summary["n_excluded_stale"] == 1
    assert summary["n_excluded_absence"] == 1
    assert summary["n_polls_failed"] == 1


def test_wall_clock_fallback_refuses_to_declare_frequent():
    # Guard 1: without vehicle timestamps every gap collapses to the poll interval.
    rows = []
    for i in range(6):
        row = sample(i, 1000 + i * 5, lat=34.0 + 0.001 * i)
        row["vehicle_timestamp"] = None
        row["header_timestamp"] = None
        rows.append(row)
    gaps = compute_gaps(pd.DataFrame(rows), ok_log(6), CRITERIA)
    summary = summarize_feed(FEED, gaps, ok_log(6), make_config(), FeedTarget(FEED, "ok", url="u"))
    assert summary["timestamp_source_mode"] == "wall_clock"
    assert summary["status"] == STATUS_DEGRADED_TIMESTAMPS
    assert summary["is_frequent"] is None


def test_resolution_limited_flag():
    # Guard 2: p50 gap at one poll interval means we never resolved the real cadence.
    rows = [sample(i, 1000 + i * 5, lat=34.0 + 0.001 * i) for i in range(6)]
    gaps = compute_gaps(pd.DataFrame(rows), ok_log(6), CRITERIA)
    summary = summarize_feed(FEED, gaps, ok_log(6), make_config(poll_interval=5.0), FeedTarget(FEED, "ok", url="u"))
    assert summary["quantization_ratio"] == pytest.approx(1.0)
    assert summary["resolution_limited"] is True


def test_not_resolution_limited_when_gaps_exceed_the_poll_rate():
    rows = [sample(i, 1000 + i * 30, lat=34.0 + 0.001 * i) for i in range(6)]
    gaps = compute_gaps(pd.DataFrame(rows), ok_log(6), CRITERIA)
    summary = summarize_feed(FEED, gaps, ok_log(6), make_config(poll_interval=5.0), FeedTarget(FEED, "ok", url="u"))
    assert summary["quantization_ratio"] == pytest.approx(6.0)
    assert summary["resolution_limited"] is False


def test_unstable_vehicle_ids_flag():
    # Guard 3: 20 distinct ids against a median fleet of 1 per poll means ids are churning.
    rows = []
    for i in range(20):
        rows.append(sample(i, 1000 + i * 10, vehicle_id=f"v{i}"))
        rows.append(sample(i + 1, 1010 + i * 10, vehicle_id=f"v{i}", lat=34.001))
    gaps = compute_gaps(pd.DataFrame(rows), ok_log(25), CRITERIA)
    summary = summarize_feed(FEED, gaps, ok_log(25, n_entities=1), make_config(), FeedTarget(FEED, "ok", url="u"))
    assert summary["n_distinct_vehicles"] == 20
    assert summary["median_vehicles_per_poll"] == 1.0
    assert summary["unstable_vehicle_ids"] is True


def test_median_vehicles_per_poll_comes_from_the_poll_log():
    # Samples hold only timestamp changes, so they cannot report fleet size.
    rows = [sample(0, 1000), sample(1, 1010, lat=34.001)]
    poll_log = ok_log(2, n_entities=137)
    gaps = compute_gaps(pd.DataFrame(rows), poll_log, CRITERIA)
    summary = summarize_feed(FEED, gaps, poll_log, make_config(), FeedTarget(FEED, "ok", url="u"))
    assert summary["median_vehicles_per_poll"] == 137.0


def test_skipped_feed_summarizes_without_data():
    target = FeedTarget("Auth Feed", "skipped_auth")
    summary = summarize_feed("Auth Feed", pd.DataFrame(), pd.DataFrame(), make_config(), target)
    assert summary["status"] == "skipped_auth"
    assert summary["is_frequent"] is None
    assert summary["n_update_gaps"] == 0


def test_percentile_columns_follow_the_configured_percentiles():
    rows = [sample(i, 1000 + i * 10, lat=34.0 + 0.001 * i) for i in range(5)]
    config = SearchConfig(
        feeds=(FEED,),
        sampling=SamplingConfig(duration_minutes=30, poll_interval_seconds=5),
        criteria=CRITERIA,
        output=OutputConfig(percentiles=(25, 75)),
    )
    gaps = compute_gaps(pd.DataFrame(rows), ok_log(5), CRITERIA)
    summary = summarize_feed(FEED, gaps, ok_log(5), config, FeedTarget(FEED, "ok", url="u"))
    assert "update_gap_p25" in summary and "update_gap_p75" in summary
    # p50 is still computed internally for the quantization guard.
    assert summary["quantization_ratio"] is not None
