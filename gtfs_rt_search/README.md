# gtfs_rt_search

Which GTFS-Realtime VehiclePositions feeds actually publish fresh positions often enough to be
useful for speed/delay analysis?

The warehouse archive can't answer that cleanly, so this tool polls the **live** feed URLs
directly and measures the update cadence it observes.

## The measurement

For each vehicle, when its position moves at least **k** metres between two consecutive updates,
the gap between those updates must be at most **n** seconds. A feed is **frequent** when at least
`min_pass_fraction` of those movement gaps pass.

Alongside the pass/fail, every run reports p10/p50/p85 of update gaps (no movement check) and of
movement gaps, so a feed that just misses the threshold is still legible.

## Usage

```bash
cd gtfs_rt_search
python run.py --config config.yml      # or: make search
```

This uses the shared repo environment created by `make install_env` at the root, following the
same convention as `gtfs_funnel/Makefile`.

> [!WARNING]
> Do **not** run `uv sync` or `uv run` from inside this directory. The uv workspace shares one
> venv at the repo root, so syncing from a member directory prunes it to just that member's
> dependencies and uninstalls everything else. Recover with `uv sync --all-groups` from the repo
> root. `gtfs-realtime-bindings` is declared both here and in the root `dev` group, so the shared
> environment already has it.

### Tracing requests

`-v` / `--verbose` prints one line per HTTP request as it is made:

```
time          feed                           tick    since  status     size  vehicles   latency
13:08:09.450  Long Beach VehiclePositions      #0        -     200     4.4K       131      90ms
13:08:10.140  Long Beach VehiclePositions      #1    5.29s     200     4.4K       131      91ms
13:08:10.265  Antelope Valley Transit Aut.     #1    0.58s     429  http_error              0ms
              └─ 429 Client Error: Too Many Requests for url: https://api.511.org/vp?api_key=***
```

**`since` is the column to watch**: the gap since that feed's *previous actual request*, not the
intended cadence. It should track `poll_interval_seconds`; anything materially smaller is a burst.
Failures print the response detail on a continuation line of the same output, with credentials in
the URL masked.

### Request-rate protection

Two guards keep the request rate honest, which matters for feeds behind an API key. Neither ever
issues an extra request; both decline to send and record why, so suppressed ticks are visible in
`poll_log.parquet` rather than silent.

| Guard | Behaviour | Logged as |
|---|---|---|
| **Spacing floor** | Two requests to one feed can never land closer than `min_spacing_seconds`. Checked immediately before the request, *after* the semaphore — the absolute-deadline schedule alone does not prevent a burst, because a thread can wait nearly a whole interval for a slot and then find its next tick already due. | `skipped_rate` |
| **Rate-limit backoff** | A `429` or `503` suspends that feed. `Retry-After` is honoured exactly and uncapped; without it, backoff is exponential from the poll interval (5s → 10s → 20s → …) capped at `BACKOFF_MAX_SECONDS`. A success resets the ladder. | `skipped_backoff` |

The floor is `poll_interval_seconds - LATE_TOLERANCE_SECONDS` (clamped to half the interval), which
is derived rather than picked: a tick may start up to `LATE_TOLERANCE_SECONDS` late, so one firing
late followed by one firing on time are genuinely that much closer than the interval. Using the
interval itself would skip roughly half of all ticks on ordinary jitter.

A breach skips the tick rather than waiting it out. Waiting would shift the feed permanently behind
its schedule; skipping costs one sample and keeps the absolute cadence intact, matching the existing
"skip rather than catch up" rule.

`403` deliberately does **not** trigger backoff: publishers use it both for quota exhaustion and for
a bad key, and silently backing off would hide the latter.

Feed names in `config.yml` must match `dim_gtfs_datasets.name` exactly for a current
`type = 'vehicle_positions'` row. An unknown name is a hard error, not a silent skip.

Resolving names needs BigQuery credentials — run `glogin` first.

From a notebook:

```python
from config import load_config
from run import run_search

config, warnings = load_config("config.yml")
summary = run_search(config, warnings)
```

Outputs land in `outputs/<run_ts>/`: `feed_summary.parquet`, `poll_log.parquet`, `gaps.parquet`,
and `raw_samples/feed=<slug>/part-*.parquet` when `write_raw_samples` is on. Samples are flushed
during the run, so a Ctrl-C still leaves usable data.

## Reading the output

`is_frequent` is the headline, but it is `None` whenever the measurement can't support a verdict.
Three guards exist because each of these can otherwise manufacture a false "frequent":

| Field | Meaning |
|---|---|
| `status = degraded_timestamps` | The feed mostly lacks `vehicle.timestamp`, so gaps fall back to our own poll clock and every gap would look like `poll_interval`. `is_frequent` is withheld. |
| `resolution_limited` | `update_gap_p50` is at most 1.5x the poll interval — we polled too slowly to resolve the real cadence. |
| `unstable_vehicle_ids` | More than 3x as many distinct vehicle ids as the median fleet per poll. Ids are churning, so every update looks like a first sighting. |

`is_regional` marks a combined-regional feed covering several operators — don't read such a result
as one agency's cadence.

Exclusions are always counted, never silently dropped: `n_excluded_stale`, `n_excluded_absence`,
`n_excluded_failed_poll`, `n_invalid_gaps`. If those are a large share of total gaps, the pass
fraction is resting on thin data.

Feeds that never get polled still appear, with a status saying why: `skipped_auth`,
`ambiguous_name`, `unresolved_url_template`, `skipped_no_url`.

In `poll_log.parquet`, `error_type` is one of `ok`, `timeout`, `ssl`, `connection`, `http_error`,
`parse_error`, `skipped_slow`, `skipped_rate`, `skipped_backoff`. The three `skipped_*` values mean
no request was sent, so they count as failed polls and their gaps are excluded — a feed showing many
`skipped_backoff` ticks is rate limiting you, and its measurement rests on much less data than the
tick count suggests.

## Layout

| File | Role |
|---|---|
| `config.py` | YAML → frozen dataclasses, with validation and unknown-key rejection |
| `feeds.py` | Feed name → URL against `dim_gtfs_datasets` |
| `poller.py` | Fixed-cadence polling and protobuf extraction |
| `metrics.py` | Gaps, haversine, percentiles, the frequent determination |
| `run.py` | CLI and `run_search()` |

## Notes for maintainers

- **`has_authentication` is nullable and never `False`** — it is `True` or `NULL`. Since
  `bool(float("nan")) is True` in Python, a plain truthiness test would skip every feed. Use
  `feeds._is_true`.
- **Gaps pair in poll order, not timestamp order.** Sorting by timestamp would reorder a feed's
  backwards timestamp into a plausible positive gap and the `is_invalid` check could never fire.
- **`median_vehicles_per_poll` comes from `poll_log.n_entities`**, not from the samples — samples
  hold only timestamp changes and so cannot report fleet size.
- **Absence is tracked in the poller**, not derived later. The timestamp dedupe discards
  present-but-unchanged observations, so `had_absence_since_last_change` is the only surviving
  record of a vehicle vanishing from a feed.
- **Verbose tracing is redacted, the parquet outputs are not (yet).** `poller.redact` masks
  credential query parameters in printed error text, but `feed_summary.parquet` still stores the
  raw `url` (`metrics.py`) and `poll_log.parquet` still stores the raw `error_msg`. Fix that before
  enabling feeds whose URLs carry inline API keys.
- **`gtfs-realtime-bindings` is pinned `<3`.** 3.0.0 requires `protobuf>=7.34`, but the
  `google-api-core` / `googleapis-common-protos` / `grpcio-status` stack caps `protobuf<7`.
- `dim_gtfs_datasets.csv` (a full local export) is gitignored; `tests/fixtures/` holds a trimmed
  copy for the offline tests.

## Tests

```bash
pytest tests
```

All offline — no credentials, no network.
