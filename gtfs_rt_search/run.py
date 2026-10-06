"""
Entry point for the GTFS-RT frequent-feed search.

    uv run python run.py --config config.yml

Also importable for notebook use:

    from run import run_search
    from config import load_config
    config, warnings = load_config("config.yml")
    summary = run_search(config, warnings)
"""

from __future__ import annotations

import argparse
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

# Flat-module layout (like gtfs_funnel/), so make the sibling modules importable even when this
# script is invoked from the repo root rather than from its own directory.
sys.path.insert(0, str(Path(__file__).resolve().parent))

import pandas as pd  # noqa: E402
from config import SearchConfig, load_config  # noqa: E402
from feeds import fetch_feed_metadata, pollable_targets, resolve_feeds  # noqa: E402
from metrics import summarize_run  # noqa: E402
from poller import run_polling  # noqa: E402

# Columns worth seeing on the console; the parquet keeps everything.
CONSOLE_COLUMNS = [
    "feed_name",
    "status",
    "is_frequent",
    "pass_fraction",
    "n_movement_gaps",
    "movement_gap_p50",
    "movement_gap_p85",
    "update_gap_p50",
    "n_polls_ok",
    "n_polls_failed",
    "median_vehicles_per_poll",
    "is_regional",
    "resolution_limited",
    "unstable_vehicle_ids",
]


def _run_timestamp() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")


def run_search(
    config: SearchConfig,
    config_warnings: Optional[list[str]] = None,
    output_dir: Optional[Path] = None,
    metadata: Optional[pd.DataFrame] = None,
) -> pd.DataFrame:
    """
    Resolve -> poll -> summarize. Returns the per-feed summary frame and writes it to parquet.

    Pass `metadata` to supply warehouse rows directly and skip the BigQuery call (useful from a
    notebook, or against a fixture).
    """
    config_warnings = list(config_warnings or [])
    run_dir = (output_dir or config.output.dir) / _run_timestamp()
    run_dir.mkdir(parents=True, exist_ok=True)

    print(f"resolving {len(config.feeds)} feed name(s) against the warehouse ...")
    if metadata is None:
        metadata = fetch_feed_metadata(config.feeds)
    targets = resolve_feeds(config.feeds, metadata)

    to_poll = pollable_targets(targets)
    for target in targets:
        detail = f" - {'; '.join(target.notes)}" if target.notes else ""
        print(f"  [{target.status}] {target.feed_name}{detail}")
    if not to_poll:
        raise SystemExit("no pollable feeds after resolution; nothing to measure")

    for warning in config_warnings:
        print(f"  warning: {warning}")

    samples_dir = (run_dir / "raw_samples") if config.output.write_raw_samples else None
    print(
        f"polling {len(to_poll)} feed(s) for {config.sampling.duration_minutes:g} min "
        f"at {config.sampling.poll_interval_seconds:g}s "
        f"({config.n_polls} ticks per feed); writing to {run_dir}"
    )

    samples, poll_log = run_polling(to_poll, config, samples_dir)
    print(f"collected {len(samples):,} position update(s) across {len(poll_log):,} poll(s)")

    summary, gaps = summarize_run(samples, poll_log, config, targets, config_warnings)

    summary.to_parquet(run_dir / "feed_summary.parquet", index=False)
    poll_log.to_parquet(run_dir / "poll_log.parquet", index=False)
    if not gaps.empty:
        gaps.to_parquet(run_dir / "gaps.parquet", index=False)

    print()
    display_columns = [c for c in CONSOLE_COLUMNS if c in summary.columns]
    with pd.option_context("display.width", 250, "display.max_columns", None):
        print(summary[display_columns].to_string(index=False))
    print(f"\nwrote {run_dir}/feed_summary.parquet")

    return summary


def main(argv: Optional[list[str]] = None) -> int:
    parser = argparse.ArgumentParser(
        description="Measure which GTFS-RT VehiclePositions feeds publish updates frequently enough for speed analysis."
    )
    parser.add_argument("--config", default="config.yml", help="path to the YAML config (default: config.yml)")
    parser.add_argument("--output-dir", default=None, help="override output.dir from the config")
    args = parser.parse_args(argv)

    config, warnings = load_config(args.config)
    run_search(
        config,
        config_warnings=warnings,
        output_dir=Path(args.output_dir) if args.output_dir else None,
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
