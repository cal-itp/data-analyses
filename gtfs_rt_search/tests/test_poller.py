import pandas as pd
import pytest
import requests
from config import CriteriaConfig, OutputConfig, SamplingConfig, SearchConfig
from feeds import FeedTarget
from google.transit import gtfs_realtime_pb2
from poller import (
    _TRACE_NAME_WIDTH,
    SAMPLE_COLUMNS,
    _FeedWorker,
    _fit,
    parse_feed_message,
    slugify,
)

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


# -- verbose tracing -------------------------------------------------------------------


class FakeResponse:
    def __init__(self, content=b"", status_code=200, raise_exc=None):
        self.content = content
        self.status_code = status_code
        self._raise_exc = raise_exc

    def raise_for_status(self):
        if self._raise_exc is not None:
            raise self._raise_exc


class FakeSession:
    """Stands in for requests.Session so _fetch_once can run without network."""

    def __init__(self, outcome):
        self.outcome = outcome
        self.calls = 0

    def get(self, url, timeout=None, headers=None):
        self.calls += 1
        if isinstance(self.outcome, Exception):
            raise self.outcome
        return self.outcome


def count_prints(monkeypatch):
    """Capture every print call so 'one call per request' can be asserted literally."""
    calls = []
    import builtins

    real_print = builtins.print

    def spy(*args, **kwargs):
        calls.append(" ".join(str(a) for a in args))
        return real_print(*args, **kwargs)

    monkeypatch.setattr(builtins, "print", spy)
    return calls


def good_response():
    content = build_feed_message(
        [{"entity_id": "e1", "vehicle_id": "b1", "timestamp": 100, "lat": 34.0, "lon": -118.0}]
    )
    return FakeResponse(content=content, status_code=200)


def test_verbose_prints_exactly_one_line_per_request(monkeypatch):
    worker = make_worker()
    worker.verbose = True
    calls = count_prints(monkeypatch)

    worker._fetch_once(FakeSession(good_response()), 0, 0.0)
    assert len(calls) == 1

    worker._fetch_once(FakeSession(good_response()), 1, 5.0)
    assert len(calls) == 2


def test_verbose_off_prints_nothing(monkeypatch):
    worker = make_worker()
    calls = count_prints(monkeypatch)
    worker._fetch_once(FakeSession(good_response()), 0, 0.0)
    assert calls == []
    # The poll is still logged; only the printing is suppressed.
    assert len(worker.poll_log_rows) == 1


def test_verbose_line_carries_tick_status_and_vehicle_count(monkeypatch):
    worker = make_worker()
    worker.verbose = True
    calls = count_prints(monkeypatch)
    worker._fetch_once(FakeSession(good_response()), 7, 0.0)
    line = calls[0]
    assert "#7" in line
    assert "200" in line
    assert "Test Feed" in line


def test_since_column_is_blank_on_first_request_then_shows_the_gap(monkeypatch):
    worker = make_worker()
    worker.verbose = True
    calls = count_prints(monkeypatch)

    worker._fetch_once(FakeSession(good_response()), 0, 0.0)
    # Nothing to compare the first request against.
    assert "-" in calls[0]

    worker._last_request_monotonic -= 5.0  # pretend the previous request was 5s ago
    worker._fetch_once(FakeSession(good_response()), 1, 5.0)
    assert "5.0" in calls[1]


def test_error_response_is_printed_with_its_detail(monkeypatch):
    worker = make_worker()
    worker.verbose = True
    calls = count_prints(monkeypatch)

    exc = requests.exceptions.HTTPError("503 Server Error: Service Unavailable for url: http://x/vp")
    exc.response = FakeResponse(status_code=503)
    worker._fetch_once(FakeSession(exc), 3, 0.0)

    # Still one print call, with the error detail attached to the same line.
    assert len(calls) == 1
    assert "503" in calls[0]
    assert "http_error" in calls[0]
    assert "Service Unavailable" in calls[0]


def test_rate_limit_response_is_visible(monkeypatch):
    """429 is the response that matters most for API-key feeds, so it must be legible."""
    worker = make_worker()
    worker.verbose = True
    calls = count_prints(monkeypatch)

    exc = requests.exceptions.HTTPError("429 Client Error: Too Many Requests for url: http://x/vp")
    exc.response = FakeResponse(status_code=429)
    worker._fetch_once(FakeSession(exc), 4, 0.0)

    assert "429" in calls[0]
    assert "Too Many Requests" in calls[0]
    assert worker.poll_log_rows[0]["error_type"] == "http_error"
    assert worker.poll_log_rows[0]["http_status"] == 429


def test_timeout_is_traced_without_a_status(monkeypatch):
    worker = make_worker()
    worker.verbose = True
    calls = count_prints(monkeypatch)
    worker._fetch_once(FakeSession(requests.exceptions.Timeout("timed out")), 2, 0.0)
    assert len(calls) == 1
    assert "timeout" in calls[0]


def test_fetch_never_raises_and_always_logs_one_row():
    """The finally block must log exactly one poll row whatever the outcome."""
    for outcome in (
        good_response(),
        requests.exceptions.Timeout("timed out"),
        requests.exceptions.SSLError("bad cert"),
        FakeResponse(content=b"not a protobuf", status_code=200),
    ):
        worker = make_worker()
        worker._fetch_once(FakeSession(outcome), 0, 0.0)
        assert len(worker.poll_log_rows) == 1


def test_long_feed_names_are_truncated_to_keep_columns_aligned(monkeypatch):
    long_name = "Antelope Valley Transit Authority Vehicle Positions"
    assert len(_fit(long_name, _TRACE_NAME_WIDTH)) == _TRACE_NAME_WIDTH
    assert len(_fit("short", _TRACE_NAME_WIDTH)) == _TRACE_NAME_WIDTH

    worker = make_worker()
    worker.target = FeedTarget(long_name, "ok", url="http://x/vp")
    worker.verbose = True
    calls = count_prints(monkeypatch)
    worker._fetch_once(FakeSession(good_response()), 0, 0.0)
    assert long_name[:20] in calls[0]


def test_redact_masks_credentials_in_urls():
    from poller import redact

    assert redact("...for url: https://api.511.org/vp?api_key=abc123&agency=EM") == (
        "...for url: https://api.511.org/vp?api_key=***&agency=EM"
    )
    # Case and separator variations publishers actually use.
    for param in ("API_KEY", "apikey", "api-key", "token", "access_token", "secret", "auth"):
        assert "***" in redact(f"https://x/vp?{param}=s3cret")
        assert "s3cret" not in redact(f"https://x/vp?{param}=s3cret")
    # Non-secret params are left alone.
    assert redact("https://x/vp?agency=EM&format=pb") == "https://x/vp?agency=EM&format=pb"
    assert redact(None) is None
    assert redact("") == ""


def test_api_key_is_not_printed_in_a_traced_error(monkeypatch):
    """Verbose mode exists to watch keyed feeds, so it must not echo the key."""
    worker = make_worker()
    worker.target = FeedTarget("Keyed Feed", "ok", url="https://api.511.org/vp?api_key=SUPERSECRET")
    worker.verbose = True
    calls = count_prints(monkeypatch)

    exc = requests.exceptions.HTTPError(
        "429 Client Error: Too Many Requests for url: https://api.511.org/vp?api_key=SUPERSECRET"
    )
    exc.response = FakeResponse(status_code=429)
    worker._fetch_once(FakeSession(exc), 0, 0.0)

    assert "SUPERSECRET" not in calls[0]
    assert "api_key=***" in calls[0]
    assert "429" in calls[0]


def test_error_rows_keep_the_latency_column_aligned(monkeypatch):
    """A long error_type must not shunt later columns out of line."""
    from poller import TRACE_HEADER

    worker = make_worker()
    worker.verbose = True
    calls = count_prints(monkeypatch)
    worker._fetch_once(FakeSession(requests.exceptions.ConnectionError("no route")), 0, 0.0)

    first_line = calls[0].split("\n")[0]
    assert len(first_line) == len(TRACE_HEADER), (len(first_line), len(TRACE_HEADER))
