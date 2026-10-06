import pandas as pd
import pytest
from config import CriteriaConfig, OutputConfig, SamplingConfig, SearchConfig
from feeds import FeedTarget
from google.transit import gtfs_realtime_pb2
from poller import SAMPLE_COLUMNS, _FeedWorker, parse_feed_message, slugify

FEED = "Test Feed"


def build_feed_message(entities, header_timestamp=1_700_000_000):
    """Serialize a FeedMessage from (id, vehicle_id, label, timestamp, lat, lon, trip_id) tuples."""
    message = gtfs_realtime_pb2.FeedMessage()
    message.header.gtfs_realtime_version = "2.0"
    if header_timestamp is not None:
        message.header.timestamp = header_timestamp

    for spec in entities:
        entity = message.entity.add()
        entity.id = spec["entity_id"]
        vp = entity.vehicle
        if spec.get("vehicle_id"):
            vp.vehicle.id = spec["vehicle_id"]
        if spec.get("label"):
            vp.vehicle.label = spec["label"]
        if spec.get("timestamp") is not None:
            vp.timestamp = spec["timestamp"]
        if spec.get("lat") is not None:
            vp.position.latitude = spec["lat"]
            vp.position.longitude = spec["lon"]
        if spec.get("bearing") is not None:
            vp.position.bearing = spec["bearing"]
        if spec.get("trip_id"):
            vp.trip.trip_id = spec["trip_id"]
    return message.SerializeToString()


# -- protobuf extraction ---------------------------------------------------------------


def test_parses_a_basic_vehicle():
    content = build_feed_message(
        [{"entity_id": "e1", "vehicle_id": "bus1", "timestamp": 1_700_000_005, "lat": 34.05, "lon": -118.25}]
    )
    records, header_ts = parse_feed_message(content)
    assert header_ts == 1_700_000_000
    assert len(records) == 1
    record = records[0]
    assert record.vehicle_id == "bus1"
    assert record.vehicle_id_source == "vehicle.id"
    assert record.vehicle_timestamp == 1_700_000_005
    assert record.latitude == pytest.approx(34.05, rel=1e-5)
    assert record.header_timestamp == 1_700_000_000


def test_entity_without_vehicle_timestamp_still_parses():
    content = build_feed_message([{"entity_id": "e1", "vehicle_id": "bus1", "lat": 34.0, "lon": -118.0}])
    records, _ = parse_feed_message(content)
    # Missing timestamp must be None, not 0, so the header can be used as the fallback clock.
    assert records[0].vehicle_timestamp is None


def test_vehicle_id_falls_back_through_label_trip_then_entity():
    content = build_feed_message(
        [
            {"entity_id": "e1", "label": "Bus 7", "timestamp": 1, "lat": 34.0, "lon": -118.0},
            {"entity_id": "e2", "trip_id": "trip-9", "timestamp": 1, "lat": 34.0, "lon": -118.0},
            {"entity_id": "e3", "timestamp": 1, "lat": 34.0, "lon": -118.0},
        ]
    )
    records, _ = parse_feed_message(content)
    assert [(r.vehicle_id, r.vehicle_id_source) for r in records] == [
        ("Bus 7", "vehicle.label"),
        ("trip-9", "trip.trip_id"),
        ("e3", "entity.id"),
    ]


def test_entity_with_no_identifier_at_all_is_dropped():
    message = gtfs_realtime_pb2.FeedMessage()
    message.header.gtfs_realtime_version = "2.0"
    entity = message.entity.add()
    entity.id = ""
    entity.vehicle.timestamp = 5
    records, _ = parse_feed_message(message.SerializeToString())
    assert records == []


def test_trip_update_only_entities_are_ignored():
    message = gtfs_realtime_pb2.FeedMessage()
    message.header.gtfs_realtime_version = "2.0"
    entity = message.entity.add()
    entity.id = "tu1"
    entity.trip_update.trip.trip_id = "t1"
    records, _ = parse_feed_message(message.SerializeToString())
    assert records == []


def test_missing_header_timestamp_is_none():
    content = build_feed_message(
        [{"entity_id": "e1", "vehicle_id": "b1", "timestamp": 5, "lat": 34.0, "lon": -118.0}],
        header_timestamp=None,
    )
    _, header_ts = parse_feed_message(content)
    assert header_ts is None


def test_malformed_protobuf_raises():
    with pytest.raises(Exception):
        parse_feed_message(b"this is not a protobuf at all, not even close")


def test_slugify():
    assert slugify("VCTC Vehicle Positions") == "vctc-vehicle-positions"
    assert slugify("Emery Go-Round VehiclePositions") == "emery-go-round-vehiclepositions"


# -- dedupe and absence tracking -------------------------------------------------------


def make_worker():
    config = SearchConfig(
        feeds=(FEED,),
        sampling=SamplingConfig(duration_minutes=1, poll_interval_seconds=5, flush_every_polls=0),
        criteria=CriteriaConfig(movement_threshold_meters=10.0, max_update_gap_seconds=20.0),
        output=OutputConfig(),
    )
    import threading

    return _FeedWorker(
        FeedTarget(FEED, "ok", url="https://example.test/vp"),
        config,
        threading.BoundedSemaphore(1),
        None,
        threading.Event(),
    )


def records_from(entities):
    return parse_feed_message(build_feed_message(entities))[0]


def test_unchanged_timestamp_is_not_recorded_twice():
    worker = make_worker()
    entities = [{"entity_id": "e1", "vehicle_id": "b1", "timestamp": 100, "lat": 34.0, "lon": -118.0}]
    worker._record_entities(0, 0.0, 1000.0, records_from(entities))
    worker._record_entities(1, 5.0, 1005.0, records_from(entities))
    assert len(worker.sample_rows) == 1


def test_changed_timestamp_is_recorded():
    worker = make_worker()
    worker._record_entities(
        0,
        0.0,
        1000.0,
        records_from([{"entity_id": "e1", "vehicle_id": "b1", "timestamp": 100, "lat": 34.0, "lon": -118.0}]),
    )
    worker._record_entities(
        1,
        5.0,
        1005.0,
        records_from([{"entity_id": "e1", "vehicle_id": "b1", "timestamp": 105, "lat": 34.001, "lon": -118.0}]),
    )
    assert len(worker.sample_rows) == 2
    assert [r["vehicle_timestamp"] for r in worker.sample_rows] == [100, 105]


def test_absence_is_flagged_on_the_next_changed_sample():
    """
    The dedupe above discards present-but-unchanged observations, so absence has to be tracked
    here, while presence is still known. This is the information compute_gaps relies on.
    """
    worker = make_worker()
    present = [{"entity_id": "e1", "vehicle_id": "b1", "timestamp": 100, "lat": 34.0, "lon": -118.0}]
    moved = [{"entity_id": "e1", "vehicle_id": "b1", "timestamp": 115, "lat": 34.001, "lon": -118.0}]

    worker._record_entities(0, 0.0, 1000.0, records_from(present))
    worker._record_entities(1, 5.0, 1005.0, [])  # vehicle missing from this poll
    worker._record_entities(2, 10.0, 1010.0, records_from(moved))

    assert len(worker.sample_rows) == 2
    assert worker.sample_rows[0]["had_absence_since_last_change"] is False
    assert worker.sample_rows[1]["had_absence_since_last_change"] is True


def test_continuous_presence_sets_no_absence_flag():
    worker = make_worker()
    worker._record_entities(
        0,
        0.0,
        1000.0,
        records_from([{"entity_id": "e1", "vehicle_id": "b1", "timestamp": 100, "lat": 34.0, "lon": -118.0}]),
    )
    # Present but unchanged: recorded nowhere, yet must not count as an absence.
    worker._record_entities(
        1,
        5.0,
        1005.0,
        records_from([{"entity_id": "e1", "vehicle_id": "b1", "timestamp": 100, "lat": 34.0, "lon": -118.0}]),
    )
    worker._record_entities(
        2,
        10.0,
        1010.0,
        records_from([{"entity_id": "e1", "vehicle_id": "b1", "timestamp": 110, "lat": 34.001, "lon": -118.0}]),
    )
    assert [r["had_absence_since_last_change"] for r in worker.sample_rows] == [False, False]


def test_absence_flag_is_consumed_once():
    worker = make_worker()

    def specs(ts, lat):
        return [{"entity_id": "e1", "vehicle_id": "b1", "timestamp": ts, "lat": lat, "lon": -118.0}]

    worker._record_entities(0, 0.0, 1000.0, records_from(specs(100, 34.0)))
    worker._record_entities(1, 5.0, 1005.0, [])
    worker._record_entities(2, 10.0, 1010.0, records_from(specs(110, 34.001)))
    worker._record_entities(3, 15.0, 1015.0, records_from(specs(120, 34.002)))
    assert [r["had_absence_since_last_change"] for r in worker.sample_rows] == [False, True, False]


def test_sample_rows_match_the_declared_schema():
    worker = make_worker()
    worker._record_entities(
        0,
        0.0,
        1000.0,
        records_from([{"entity_id": "e1", "vehicle_id": "b1", "timestamp": 100, "lat": 34.0, "lon": -118.0}]),
    )
    assert set(worker.sample_rows[0]) == set(SAMPLE_COLUMNS)
    # And the frame built from them is well-formed.
    df = pd.DataFrame(worker.sample_rows, columns=SAMPLE_COLUMNS)
    assert list(df.columns) == SAMPLE_COLUMNS
