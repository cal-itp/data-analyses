# GTFS-RT Vehicle Positions "frequent feed" search tool

## Context

We want to know which GTFS-Realtime VehiclePositions feeds actually publish fresh positions
often enough to be useful for speed/delay analysis. The warehouse archive can't answer this
cleanly, so this tool polls the **live** feed URLs directly and measures observed update cadence.

The measurement is: for each vehicle, when its position moves at least **k** meters between two
consecutive updates, the gap between those updates must be at most **n** seconds. A feed is
**frequent** if at least `min_pass_fraction` of such movement gaps pass. Alongside the pass/fail,
the tool reports p10/p50/p85 of update gaps (no movement check) and of movement gaps.

Feed names come from the warehouse (`vehicle_positions_gtfs_dataset_name`); everything else is
driven by one YAML config. Nothing like this exists in the repo yet — there is **no** GTFS-RT
protobuf code anywhere in `data-analyses` today, so this is the first live-feed fetcher.

Outcome: `uv run python run.py --config config.yml` produces a per-feed parquet summary plus a
console table, and `run_search(config)` is importable for notebook use.

## Decisions already made (do not re-litigate)

| Question | Decision |
|---|---|
| "New update" | per vehicle id, `VehiclePosition.timestamp` differs from last seen |
| Gap clock | effective timestamp = `vehicle.timestamp` → `header.timestamp` → poll wall-clock |
| Percentile grain | all per-vehicle gaps pooled per feed |
| Frequent rule | `fraction(movement gaps <= n) >= min_pass_fraction` |
| Feed selection | YAML lists exact names only; unknown names are a hard error |
| Auth feeds | `has_authentication = TRUE` → skipped, `status = "skipped_auth"` |
| Stale gaps | drop gaps over a cutoff **and** gaps straddling a failed/skipped poll; count them |
| Durability | flush raw samples to parquet parts periodically |
| Output | parquet (summary + raw samples + poll log) and a printed table |
| Entry point | `run.py` CLI + importable `run_search()` |

## Layout

New files in `/workspaces/data-analyses/gtfs_rt_search/` (currently empty, flat-module style like
`gtfs_funnel/`). Black line-length 120, isort `--profile black`, flake8 with the repo's ignore list.

```
gtfs_rt_search/
  pyproject.toml      # uv workspace member
  config.yml          # example / default config
  config.py           # dataclasses + YAML load & validation
  feeds.py            # warehouse name -> URL resolution
  poller.py           # fixed-cadence concurrent polling + protobuf extraction
  metrics.py          # gaps, haversine, percentiles, frequent determination
  run.py              # CLI + run_search()
  README.md
  tests/              # pure-function pytest, no network
```

### Packaging

`gtfs_rt_search/pyproject.toml`, mirroring `sb125_analyses/pyproject.toml`:

```toml
[project]
name = "gtfs-rt-search"
version = "0.1.0"
requires-python = ">=3.11.0, <3.12.0"
dependencies = [
    "calitp-data-analysis",     # BigQuery engine; brings pandas + requests
    "gtfs-realtime-bindings",   # NEW to this repo
    "pyyaml",
    "pyarrow",
]

[tool.uv]
package = false

[tool.uv.sources]
calitp-data-analysis = { workspace = true }
```

Then add `"gtfs_rt_search"` to `[tool.uv.workspace] members` in the root
`/workspaces/data-analyses/pyproject.toml` and run `uv sync`. Do **not** depend on `shared-utils`
— it drags in dask/dask_geopandas/geopandas for no benefit here.

### `config.yml`

```yaml
feeds:
  - Bay Area 511 Regional VehiclePositions
  - LA Metro Bus Vehicle Positions

sampling:
  duration_minutes: 30
  poll_interval_seconds: 5
  request_timeout_seconds: 10
  max_concurrent_requests: 10
  flush_every_polls: 200

criteria:
  movement_threshold_meters: 10     # k
  max_update_gap_seconds: 20        # n
  min_pass_fraction: 0.9
  stale_gap_seconds: 600            # drop gaps longer than this
  exclude_absence_gaps: true        # drop gaps straddling a failed/skipped poll or vehicle absence

output:
  dir: ./outputs
  percentiles: [10, 50, 85]
  write_raw_samples: true
```

`config.py`: frozen dataclasses (`SearchConfig`), `load_config(path) -> tuple[SearchConfig, list[str]]`.
Validate — non-empty unique `feeds`; all numerics > 0; `0 < min_pass_fraction <= 1`; **reject unknown
YAML keys** as a typo guard. Warn (and carry the warning into the summary as `poll_interval_warning`)
when `poll_interval_seconds > max_update_gap_seconds / 2`, since observed gaps quantize to the poll
rate. No pydantic — it's only in the root `portfolio` dependency group.

## Feed resolution — `feeds.py`

Use plain SQL through the repo's standard engine: `from calitp_data_analysis.sql import get_engine, query_sql`
(`calitp-data-analysis/calitp_data_analysis/sql.py:22,86`). Set `CALITP_BQ_MAX_BYTES` in the env
before import, or pass `get_engine(max_bytes=...)` explicitly — `sql.py:10` reads it at import time.

Deliberately **not** using `shared_utils.schedule_rt_utils.filter_dim_gtfs_datasets()`
(`_shared_utils/shared_utils/schedule_rt_utils.py:73`): it hard-filters `data_quality_pipeline == True`,
which would silently drop feeds the user asked for by name.

`query_sql` has no bind parameters, so build the `IN` list by quoting names and rejecting any name
containing `'`, `\`, or a newline.

```sql
WITH ds AS (
  SELECT key, name, uri, base64_url, has_authentication, private_dataset,
         deprecated_date, data_quality_pipeline
  FROM `cal-itp-data-infra.mart_transit_database.dim_gtfs_datasets`
  WHERE _is_current AND type = 'vehicle_positions' AND name IN ({{ names }})
), prov AS (
  SELECT vehicle_positions_gtfs_dataset_key AS key,
         ARRAY_AGG(DISTINCT organization_name IGNORE NULLS ORDER BY organization_name LIMIT 50) AS organization_names,
         ARRAY_AGG(DISTINCT regional_feed_type IGNORE NULLS LIMIT 10) AS regional_feed_types,
         LOGICAL_OR(gtfs_service_data_customer_facing) AS any_customer_facing,
         COUNT(DISTINCT organization_name) AS n_organizations
  FROM `cal-itp-data-infra.mart_transit_database.dim_provider_gtfs_data`
  WHERE _is_current AND vehicle_positions_gtfs_dataset_key IS NOT NULL
  GROUP BY 1
)
SELECT ds.*, prov.* EXCEPT (key)
FROM ds LEFT JOIN prov USING (key)
```

Pre-aggregating `prov` in its own CTE is what prevents the documented many-to-many fanout of
`dim_provider_gtfs_data` — never join raw and dedupe afterwards. Filter on `_is_current` only; do
not also constrain `_valid_from`/`_valid_to`.

Post-query in Python:
- requested-minus-returned → raise with the missing names listed (hard error, per decision)
- duplicate `name` → `status = "ambiguous_name"`, list the keys, skip
- `has_authentication` → `status = "skipped_auth"`, skip
- `private_dataset` or non-null `deprecated_date` → warn but still poll
- URL = `uri`; fall back to `base64.urlsafe_b64decode(base64_url)` when `uri` is null
- dedupe identical URLs so a shared URL is fetched once
- carry `n_organizations > 1` or non-null `regional_feed_type` through as `is_regional`, so nobody
  reads a combined-regional result as per-agency

## Polling — `poller.py`

`run_polling(feeds, config) -> (samples_df, poll_log_df)`.

- `ThreadPoolExecutor(max_concurrent_requests)`, one `requests.Session` per worker thread
  (sessions are not thread-safe). `REQUESTS_CA_BUNDLE` is already set by the devcontainer, so the
  Caltrans SSL-inspection cert is handled; catch `requests.exceptions.SSLError` as its own
  `error_type` so TLS problems are legible rather than a generic failure.
- No-drift cadence: anchor on one `time.monotonic()`, precompute deadlines `t0 + i * interval`,
  `sleep(max(0, deadline - monotonic()))`. If a deadline has already passed, **skip** that tick
  (`status = "skipped_slow"`) rather than catching up. A slow feed must not stall the cadence.
- Parse with `google.transit.gtfs_realtime_pb2.FeedMessage().ParseFromString(resp.content)`.
  Per entity extract: vehicle id (`entity.vehicle.vehicle.id` → `.label` → `trip.trip_id` →
  `entity.id`, recording `vehicle_id_source`), `vehicle.timestamp`, `header.timestamp`, lat, lon,
  bearing, speed, `current_status`, `trip_id`.
- Memory: keep a per-feed `dict[vehicle_id -> last_timestamp]` in the worker and **append only
  records whose timestamp changed**. 60 min @ 5s × 20 feeds is ~14k polls; dedupe at write time is
  what keeps this to a couple million rows instead of tens of millions. Buffer plain tuples, flush
  to `raw_samples/feed=<slug>/part-<seq>.parquet` every `flush_every_polls`, clear the buffer.
  Metrics read the parts back at the end, so Ctrl-C mid-run still leaves usable data.
- Poll log row per (feed, tick): `feed_name, poll_seq, scheduled_at, requested_at, http_status,
  n_bytes, latency_ms, error_type, error_msg, n_entities` with
  `error_type ∈ {ok, timeout, ssl, connection, http_error, parse_error, skipped_slow}`.

## Metrics — `metrics.py`

- `haversine_m(lat1, lon1, lat2, lon2)` — vectorized numpy float64. No geopandas/pyproj.
- `assign_effective_timestamp(df)` — `vehicle.timestamp` → `header.timestamp` → `requested_at`,
  recording `timestamp_source` per row.
- Within a poll, keep max `effective_timestamp` per `(feed, poll_seq, vehicle_id)` — some feeds
  emit a vehicle twice.
- `compute_gaps(samples)` — sort by `(feed_name, vehicle_id, effective_timestamp)`, group by
  `(feed_name, vehicle_id)`, `.diff()` for `gap_seconds`, shifted lat/lon for `distance_m`.
  One row per consecutive-update pair. Flags per gap:
  - `is_movement = distance_m >= k`
  - `passes = gap_seconds <= n`
  - `straddles_failed_poll` — joined off the poll log: the interval overlaps a tick where this
    feed's fetch failed or was skipped
  - `straddles_absence` — the vehicle was missing from at least one intervening successful poll
  - `is_stale = gap_seconds > stale_gap_seconds`
  - `is_invalid = gap_seconds <= 0` (non-monotonic) or source timestamp beyond `requested_at + 60s`
  Excluded from both distributions and from the frequent determination: `is_invalid`, `is_stale`,
  and (when `exclude_absence_gaps`) `straddles_failed_poll` / `straddles_absence`. All exclusions
  counted and reported, never silently dropped.
- `summarize_feed(gaps, poll_log, config) -> dict`:
  `status, n_polls_attempted, n_polls_ok, n_polls_failed, n_distinct_vehicles,
  median_vehicles_per_poll, n_updates, n_update_gaps, update_gap_p10/p50/p85, n_movement_gaps,
  movement_gap_p10/p50/p85, pass_fraction, is_frequent, n_excluded_stale, n_excluded_absence,
  n_excluded_failed_poll, n_invalid_gaps, timestamp_source_mode, vehicle_id_source_mode,
  quantization_ratio, is_regional, organization_names, poll_interval_warning, error_summary`.

Three honesty guards, because each one can otherwise manufacture a false "frequent":
1. If a feed's dominant `timestamp_source` is not `vehicle`, set `status = "degraded_timestamps"`
   and report `is_frequent` as `None` — a wall-clock fallback makes every gap ≈ `poll_interval`.
2. `quantization_ratio = update_gap_p50 / poll_interval_seconds`; flag `resolution_limited` when
   it is <= 1.5 — we polled too slowly to resolve the real cadence.
3. If `n_distinct_vehicles > 3 × median_vehicles_per_poll`, flag `unstable_vehicle_ids` — ids are
   churning, so every update looks like a first sighting and gap counts collapse.

## Entry point — `run.py`

`run_search(config: SearchConfig) -> pd.DataFrame` orchestrates resolve → poll → summarize, writes
`outputs/<run_ts>/feed_summary.parquet`, `poll_log.parquet`, `raw_samples/` (when enabled), prints
the summary with `df.to_string()` (no new console library), and returns the summary frame.
`python run.py --config config.yml` via `argparse`.

## Tests — `gtfs_rt_search/tests/`

Pure functions only, no network (layout follows `_shared_utils/tests/`):
- `compute_gaps` on a hand-built frame containing a dropout, a negative gap, a stale gap, and a
  gap straddling a failed poll — assert exactly which are excluded
- `haversine_m` against a couple of known distances
- `summarize_feed` pass-fraction boundary at exactly `n` and exactly `k`
- `load_config` rejections: unknown key, zero interval, bad fraction; and the `n/2` warning
- protobuf → rows extraction against a small serialized `FeedMessage` fixture, including an entity
  with no `vehicle.timestamp` and one with no vehicle id
- feed-resolution post-processing given a stub DataFrame: not-found, ambiguous, auth-skip

## Verification

1. `uv sync` from `/workspaces/data-analyses` after adding the workspace member; confirm
   `gtfs-realtime-bindings` resolves and the root `uv.lock` updates.
2. `glogin` (the repo's `gcloud auth application-default login` wrapper — see
   `.devcontainer/flake.nix:25`). **There are currently no ADC credentials in this container**, so
   the warehouse query will fail until this is run. This step needs the user.
3. Feed resolution alone: short script / `python -c` calling `feeds.resolve_feeds([...])` with two
   known names plus one bogus name — confirm the URLs come back, the bogus name raises, and no
   fanout duplicate rows appear.
4. `pytest gtfs_rt_search/tests` — all offline.
5. Smoke run: `config.yml` with 2 open feeds, `duration_minutes: 2`, `poll_interval_seconds: 5`.
   Confirm parquet parts appear during the run, the console table prints, update-gap p50 is
   plausible (feeds commonly land in the 15–60s range), and `timestamp_source_mode == "vehicle"`.
6. Failure-path check: add a deliberately bad URL feed entry and confirm it reports an `error_type`
   and does not crash the run or stall the cadence.
7. Real run at the user's intended duration; sanity-check that `is_frequent` feeds have
   `movement_gap_p85 <= n` and that exclusion counts are a small share of total gaps.
8. `pre-commit run --files gtfs_rt_search/*` for black/isort/flake8.
