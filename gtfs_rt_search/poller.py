"""
Poll live GTFS-RT VehiclePositions feeds at a fixed cadence and record position updates.

One thread per feed, each owning its own session and state, with a semaphore capping concurrent
HTTP calls. A shared worker pool would backlog as soon as feeds outnumber workers, which is
exactly the case this tool runs in, and a slow feed must never stall another feed's cadence.
"""

from __future__ import annotations

import dataclasses
import re
import threading
import time
from datetime import datetime
from pathlib import Path
from typing import Any, Optional

import pandas as pd
import requests
from config import SearchConfig
from feeds import FeedTarget
from google.transit import gtfs_realtime_pb2

# How late a tick may start before we give up on it rather than chase the schedule.
LATE_TOLERANCE_SECONDS = 0.5

# Verbose tracing is emitted from every feed thread at once, so serialise it: without this the
# lines interleave and the timings become unreadable, which defeats the point of tracing them.
_TRACE_LOCK = threading.Lock()

# Width of the feed-name column in verbose output.
_TRACE_NAME_WIDTH = 28

TRACE_HEADER = (
    f"{'time':<12}  {'feed':<{_TRACE_NAME_WIDTH}}  {'tick':>5}  {'since':>7}  "
    f"{'status':>6}  {'size':>7}  {'vehicles':>8}  {'latency':>8}"
)

ERROR_OK = "ok"
ERROR_TIMEOUT = "timeout"
ERROR_SSL = "ssl"
ERROR_CONNECTION = "connection"
ERROR_HTTP = "http_error"
ERROR_PARSE = "parse_error"
ERROR_SKIPPED_SLOW = "skipped_slow"

POLL_LOG_COLUMNS = [
    "feed_name",
    "poll_seq",
    "scheduled_at",
    "requested_at",
    "http_status",
    "n_bytes",
    "latency_ms",
    "error_type",
    "error_msg",
    "n_entities",
]

SAMPLE_COLUMNS = [
    "feed_name",
    "poll_seq",
    "scheduled_at",
    "requested_at",
    "vehicle_id",
    "vehicle_id_source",
    "vehicle_timestamp",
    "header_timestamp",
    "latitude",
    "longitude",
    "bearing",
    "speed",
    "current_status",
    "trip_id",
    "had_absence_since_last_change",
]

# Explicit dtypes keep every flushed part schema-identical. Without this, a part where every
# bearing came back null lands as all-NA and pandas warns (and will change dtype) on concat.
SAMPLE_DTYPES = {
    "feed_name": "object",
    "poll_seq": "int64",
    "scheduled_at": "float64",
    "requested_at": "float64",
    "vehicle_id": "object",
    "vehicle_id_source": "object",
    "vehicle_timestamp": "Int64",
    "header_timestamp": "Int64",
    "latitude": "float64",
    "longitude": "float64",
    "bearing": "float64",
    "speed": "float64",
    "current_status": "Int64",
    "trip_id": "object",
    "had_absence_since_last_change": "bool",
}

_SLUG_RE = re.compile(r"[^a-z0-9]+")


def sample_frame(rows: list[dict[str, Any]]) -> pd.DataFrame:
    """Build a sample frame with the declared schema, empty or not."""
    df = pd.DataFrame(rows, columns=SAMPLE_COLUMNS)
    return df.astype(SAMPLE_DTYPES)


def slugify(name: str) -> str:
    """Filesystem-safe feed name for a parquet partition directory."""
    return _SLUG_RE.sub("-", name.lower()).strip("-") or "feed"


def _fit(text: str, width: int) -> str:
    """Pad or truncate to an exact width so verbose columns stay aligned."""
    if len(text) <= width:
        return f"{text:<{width}}"
    return text[: width - 1] + "."


# Query parameters whose values are credentials. Feed URLs carry keys inline (511 uses
# ?api_key=...), and requests embeds the full URL in its exception messages, so anything printed
# or logged has to be scrubbed first.
_SECRET_PARAM_RE = re.compile(
    r"([?&](?:api[_-]?key|key|token|access[_-]?token|secret|password|passwd|auth[a-z_]*)=)([^&\s]+)",
    re.IGNORECASE,
)


def redact(text: Optional[str]) -> Optional[str]:
    """Mask credential values in any URL appearing in text."""
    if not text:
        return text
    return _SECRET_PARAM_RE.sub(r"\1***", text)


def _format_size(n_bytes: Optional[int]) -> str:
    if n_bytes is None:
        return f"{'-':>7}"
    if n_bytes < 1024:
        return f"{n_bytes:6d}B"
    if n_bytes < 1024 * 1024:
        return f"{n_bytes / 1024:6.1f}K"
    return f"{n_bytes / (1024 * 1024):6.1f}M"


@dataclasses.dataclass
class EntityRecord:
    """One vehicle's position as published in a single poll."""

    vehicle_id: str
    vehicle_id_source: str
    vehicle_timestamp: Optional[int]
    header_timestamp: Optional[int]
    latitude: Optional[float]
    longitude: Optional[float]
    bearing: Optional[float]
    speed: Optional[float]
    current_status: Optional[int]
    trip_id: Optional[str]


def _resolve_vehicle_id(entity) -> tuple[Optional[str], Optional[str]]:
    """
    Pick the most stable identifier a feed offers, recording which one we used.

    Falling back to trip_id or the entity id changes what a "vehicle" means, so the source is
    carried through to the summary rather than silently assumed.
    """
    vp = entity.vehicle
    if vp.HasField("vehicle"):
        descriptor = vp.vehicle
        if descriptor.id:
            return descriptor.id, "vehicle.id"
        if descriptor.label:
            return descriptor.label, "vehicle.label"
    if vp.HasField("trip") and vp.trip.trip_id:
        return vp.trip.trip_id, "trip.trip_id"
    if entity.id:
        return entity.id, "entity.id"
    return None, None


def parse_feed_message(content: bytes) -> tuple[list[EntityRecord], Optional[int]]:
    """
    Parse a serialized FeedMessage into per-vehicle records plus the header timestamp.

    Raises on malformed protobuf; the caller logs that as a parse_error so a feed publishing
    junk is distinguishable from one that is merely slow.
    """
    message = gtfs_realtime_pb2.FeedMessage()
    message.ParseFromString(content)

    header_timestamp = int(message.header.timestamp) if message.header.timestamp else None

    records: list[EntityRecord] = []
    for entity in message.entity:
        if not entity.HasField("vehicle"):
            continue
        vehicle_id, id_source = _resolve_vehicle_id(entity)
        if vehicle_id is None:
            continue

        vp = entity.vehicle
        position = vp.position if vp.HasField("position") else None
        records.append(
            EntityRecord(
                vehicle_id=vehicle_id,
                vehicle_id_source=id_source,
                vehicle_timestamp=int(vp.timestamp) if vp.timestamp else None,
                header_timestamp=header_timestamp,
                latitude=float(position.latitude) if position is not None else None,
                longitude=float(position.longitude) if position is not None else None,
                bearing=float(position.bearing) if position is not None and position.HasField("bearing") else None,
                speed=float(position.speed) if position is not None and position.HasField("speed") else None,
                current_status=int(vp.current_status) if vp.HasField("current_status") else None,
                trip_id=vp.trip.trip_id if (vp.HasField("trip") and vp.trip.trip_id) else None,
            )
        )
    return records, header_timestamp


def _classify_error(exc: Exception) -> tuple[str, str]:
    """Map a requests exception to a legible error_type. SSL gets its own bucket."""
    if isinstance(exc, requests.exceptions.SSLError):
        return ERROR_SSL, str(exc)[:300]
    if isinstance(exc, requests.exceptions.Timeout):
        return ERROR_TIMEOUT, str(exc)[:300]
    if isinstance(exc, requests.exceptions.ConnectionError):
        return ERROR_CONNECTION, str(exc)[:300]
    if isinstance(exc, requests.exceptions.HTTPError):
        return ERROR_HTTP, str(exc)[:300]
    return ERROR_PARSE, f"{type(exc).__name__}: {exc}"[:300]


class _FeedWorker:
    """
    Polls one feed on its own schedule.

    Holds the per-vehicle last-seen timestamp so only *changes* are written - a 30 min run at 5s
    would otherwise record the same stationary vehicle hundreds of times. Because that dedupe
    discards the record of a vehicle being present-but-unchanged, absence is tracked here, where
    presence is still known, and stamped onto the next changed sample.
    """

    def __init__(
        self,
        target: FeedTarget,
        config: SearchConfig,
        semaphore: threading.Semaphore,
        samples_dir: Optional[Path],
        stop_event: threading.Event,
        verbose: bool = False,
    ):
        self.target = target
        self.config = config
        self.semaphore = semaphore
        self.samples_dir = samples_dir
        self.stop_event = stop_event
        self.verbose = verbose

        self.poll_log_rows: list[dict[str, Any]] = []
        self.sample_rows: list[dict[str, Any]] = []
        self.written_parts: list[Path] = []
        self.n_samples_written = 0
        self._part_seq = 0

        self._last_timestamp: dict[str, Optional[int]] = {}
        self._last_seen_poll_seq: dict[str, int] = {}
        self._absence_pending: dict[str, bool] = {}
        self._last_ok_poll_seq: Optional[int] = None

        # When this feed last actually issued a request, for the "since" column. Measured per
        # feed, so it shows the real spacing between pings rather than the intended cadence.
        self._last_request_monotonic: Optional[float] = None

    # -- recording -------------------------------------------------------------------

    def _log_poll(self, **kwargs) -> None:
        row = {col: kwargs.get(col) for col in POLL_LOG_COLUMNS}
        row["feed_name"] = self.target.feed_name
        self.poll_log_rows.append(row)

    def _record_entities(
        self, poll_seq: int, scheduled_at: float, requested_at: float, records: list[EntityRecord]
    ) -> None:
        seen_now = set()
        for record in records:
            seen_now.add(record.vehicle_id)

            # Presence bookkeeping must happen for every vehicle in the poll, not just the ones
            # whose timestamp moved, or absence information is lost to the dedupe below.
            last_seen = self._last_seen_poll_seq.get(record.vehicle_id)
            if last_seen is not None and self._last_ok_poll_seq is not None and last_seen < self._last_ok_poll_seq:
                self._absence_pending[record.vehicle_id] = True
            self._last_seen_poll_seq[record.vehicle_id] = poll_seq

            previous = self._last_timestamp.get(record.vehicle_id, "unset")
            if previous != "unset" and previous == record.vehicle_timestamp:
                continue
            self._last_timestamp[record.vehicle_id] = record.vehicle_timestamp

            self.sample_rows.append(
                {
                    "feed_name": self.target.feed_name,
                    "poll_seq": poll_seq,
                    "scheduled_at": scheduled_at,
                    "requested_at": requested_at,
                    "vehicle_id": record.vehicle_id,
                    "vehicle_id_source": record.vehicle_id_source,
                    "vehicle_timestamp": record.vehicle_timestamp,
                    "header_timestamp": record.header_timestamp,
                    "latitude": record.latitude,
                    "longitude": record.longitude,
                    "bearing": record.bearing,
                    "speed": record.speed,
                    "current_status": record.current_status,
                    "trip_id": record.trip_id,
                    "had_absence_since_last_change": self._absence_pending.pop(record.vehicle_id, False),
                }
            )

        self._last_ok_poll_seq = poll_seq

    def _flush(self) -> None:
        """Write buffered samples to a parquet part so a Ctrl-C mid-run still leaves usable data."""
        if not self.sample_rows:
            return
        self.n_samples_written += len(self.sample_rows)
        if self.samples_dir is not None:
            part_dir = self.samples_dir / f"feed={slugify(self.target.feed_name)}"
            part_dir.mkdir(parents=True, exist_ok=True)
            path = part_dir / f"part-{self._part_seq:05d}.parquet"
            sample_frame(self.sample_rows).to_parquet(path, index=False)
            self.written_parts.append(path)
            self._part_seq += 1
        else:
            self._retained = getattr(self, "_retained", [])
            self._retained.extend(self.sample_rows)
        self.sample_rows = []

    def collected_samples(self) -> pd.DataFrame:
        """Samples held in memory (only populated when not writing parquet parts)."""
        return sample_frame(getattr(self, "_retained", []))

    # -- fetching --------------------------------------------------------------------

    def _trace(
        self,
        poll_seq: int,
        requested_at: float,
        since_last: Optional[float],
        latency_ms: Optional[float],
        http_status: Optional[int],
        n_bytes: Optional[int],
        n_entities: Optional[int],
        error_type: str,
        error_msg: Optional[str],
    ) -> None:
        """
        Emit exactly one line per request, under a lock so threads cannot interleave.

        The "since" column is the gap since this feed's previous request, which is the number to
        watch when a feed is rate limited: it should track poll_interval_seconds, and anything
        materially smaller is a burst.
        """
        stamp = datetime.fromtimestamp(requested_at).strftime("%H:%M:%S.%f")[:-3]
        since = f"{since_last:>6.2f}s" if since_last is not None else f"{'-':>7}"
        latency = f"{latency_ms:>6.0f}ms" if latency_ms is not None else f"{'-':>8}"

        line = f"{stamp:<12}  {_fit(self.target.feed_name, _TRACE_NAME_WIDTH)}  " f"{'#' + str(poll_seq):>5}  {since}  "
        if error_type == ERROR_OK:
            line += f"{http_status or 0:>6}  {_format_size(n_bytes)}  {n_entities:>8}  {latency}"
        else:
            status = str(http_status) if http_status is not None else "-"
            # error_type spans the size and vehicles columns, which are meaningless on a failure,
            # so the latency column still lines up with the header.
            line += f"{status:>6}  {error_type:<17}  {latency}"
            if error_msg:
                # Same print call, so the detail can never be split from its request line, and
                # redacted so an inline api_key never reaches the terminal or a captured log.
                line += f"\n{'':<14}└─ {redact(error_msg)}"

        with _TRACE_LOCK:
            print(line, flush=True)

    def _fetch_once(self, session: requests.Session, poll_seq: int, scheduled_at: float) -> None:
        requested_at = time.time()
        started = time.monotonic()

        since_last = None if self._last_request_monotonic is None else started - self._last_request_monotonic
        self._last_request_monotonic = started

        latency_ms: Optional[float] = None
        http_status: Optional[int] = None
        n_bytes: Optional[int] = None
        n_entities: Optional[int] = None
        error_type, error_msg = ERROR_OK, None

        try:
            response = session.get(
                self.target.url,
                timeout=self.config.sampling.request_timeout_seconds,
                headers={"Accept": "application/x-protobuf"},
            )
            latency_ms = (time.monotonic() - started) * 1000.0
            response.raise_for_status()
            records, _ = parse_feed_message(response.content)
            http_status = response.status_code
            n_bytes = len(response.content)
            n_entities = len(records)
            self._record_entities(poll_seq, scheduled_at, requested_at, records)
        except Exception as exc:  # noqa: BLE001 - every failure must be logged, never raised
            error_type, error_msg = _classify_error(exc)
            http_status = getattr(getattr(exc, "response", None), "status_code", None)
            if latency_ms is None:
                latency_ms = (time.monotonic() - started) * 1000.0
        finally:
            # One log row and, in verbose mode, one printed line per request - whatever happened.
            self._log_poll(
                poll_seq=poll_seq,
                scheduled_at=scheduled_at,
                requested_at=requested_at,
                http_status=http_status,
                n_bytes=n_bytes,
                latency_ms=latency_ms,
                error_type=error_type,
                error_msg=error_msg,
                n_entities=n_entities,
            )
            if self.verbose:
                self._trace(
                    poll_seq=poll_seq,
                    requested_at=requested_at,
                    since_last=since_last,
                    latency_ms=latency_ms,
                    http_status=http_status,
                    n_bytes=n_bytes,
                    n_entities=n_entities,
                    error_type=error_type,
                    error_msg=error_msg,
                )

    def run(self, t0: float) -> None:
        """Poll on deadlines anchored to t0 so the cadence cannot drift over a long run."""
        interval = self.config.sampling.poll_interval_seconds
        n_polls = self.config.n_polls
        flush_every = self.config.sampling.flush_every_polls

        session = requests.Session()
        try:
            for poll_seq in range(n_polls):
                if self.stop_event.is_set():
                    break

                deadline = t0 + poll_seq * interval
                now = time.monotonic()
                if now < deadline:
                    if self.stop_event.wait(timeout=deadline - now):
                        break
                    now = time.monotonic()

                scheduled_at = time.time() - (time.monotonic() - deadline)

                # Behind schedule: skip this tick outright rather than catch up, so one slow
                # response cannot shift every later deadline.
                if now > deadline + LATE_TOLERANCE_SECONDS:
                    self._log_poll(
                        poll_seq=poll_seq,
                        scheduled_at=scheduled_at,
                        requested_at=None,
                        error_type=ERROR_SKIPPED_SLOW,
                        error_msg=f"tick started {now - deadline:.2f}s late",
                    )
                    continue

                # Don't queue behind the semaphore past our next deadline either.
                budget = max(0.0, (deadline + interval) - time.monotonic())
                if not self.semaphore.acquire(timeout=budget):
                    self._log_poll(
                        poll_seq=poll_seq,
                        scheduled_at=scheduled_at,
                        requested_at=None,
                        error_type=ERROR_SKIPPED_SLOW,
                        error_msg="could not acquire a request slot before the next deadline",
                    )
                    continue
                try:
                    self._fetch_once(session, poll_seq, scheduled_at)
                finally:
                    self.semaphore.release()

                if flush_every and (poll_seq + 1) % flush_every == 0:
                    self._flush()
        finally:
            self._flush()
            session.close()


def run_polling(
    targets: list[FeedTarget],
    config: SearchConfig,
    samples_dir: Optional[Path] = None,
    verbose: bool = False,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """
    Poll every pollable target for the configured duration.

    Returns (samples_df, poll_log_df). When samples_dir is given, samples are flushed to parquet
    parts as the run proceeds and read back at the end; otherwise they are held in memory.

    With verbose, every request prints one line as it is made - see _FeedWorker._trace.
    """
    if not targets:
        return sample_frame([]), pd.DataFrame(columns=POLL_LOG_COLUMNS)

    semaphore = threading.BoundedSemaphore(config.sampling.max_concurrent_requests)
    stop_event = threading.Event()
    workers = [_FeedWorker(t, config, semaphore, samples_dir, stop_event, verbose) for t in targets]

    if verbose:
        print(TRACE_HEADER, flush=True)

    t0 = time.monotonic()
    threads = [
        threading.Thread(target=w.run, args=(t0,), name=f"poll-{slugify(w.target.feed_name)}", daemon=True)
        for w in workers
    ]
    for thread in threads:
        thread.start()
    try:
        for thread in threads:
            thread.join()
    except KeyboardInterrupt:
        # Let the workers flush what they already have rather than losing the run.
        stop_event.set()
        for thread in threads:
            thread.join(timeout=config.sampling.request_timeout_seconds + 5)
        print("\ninterrupted; flushed partial results")

    poll_log = pd.DataFrame(
        [row for w in workers for row in w.poll_log_rows],
        columns=POLL_LOG_COLUMNS,
    )

    if samples_dir is not None:
        frames = [pd.read_parquet(p) for p in (p for w in workers for p in w.written_parts)]
    else:
        frames = [w.collected_samples() for w in workers]

    # Drop empties before concatenating: a part where every bearing/speed came back null would
    # otherwise trip pandas' all-NA dtype deprecation.
    frames = [f for f in frames if not f.empty]
    samples = pd.concat(frames, ignore_index=True) if frames else sample_frame([])

    return samples, poll_log
