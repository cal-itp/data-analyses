"""
Configuration loading and validation for the GTFS-RT frequent-feed search.

One YAML file drives the whole run. Unknown keys are rejected rather than ignored, because a
typo in a threshold would silently change the measurement rather than fail.
"""

from __future__ import annotations

import dataclasses
import math
from pathlib import Path
from typing import Any

import yaml

DEFAULT_PERCENTILES = (10, 50, 85)


class ConfigError(ValueError):
    """Raised when the YAML config is missing, malformed, or internally inconsistent."""


@dataclasses.dataclass(frozen=True)
class SamplingConfig:
    duration_minutes: float
    poll_interval_seconds: float
    request_timeout_seconds: float = 10.0
    max_concurrent_requests: int = 10
    flush_every_polls: int = 200


@dataclasses.dataclass(frozen=True)
class CriteriaConfig:
    movement_threshold_meters: float  # k
    max_update_gap_seconds: float  # n
    min_pass_fraction: float = 0.9
    stale_gap_seconds: float = 600.0
    exclude_absence_gaps: bool = True


@dataclasses.dataclass(frozen=True)
class OutputConfig:
    dir: Path = Path("./outputs")
    percentiles: tuple[int, ...] = DEFAULT_PERCENTILES
    write_raw_samples: bool = True


@dataclasses.dataclass(frozen=True)
class SearchConfig:
    feeds: tuple[str, ...]
    sampling: SamplingConfig
    criteria: CriteriaConfig
    output: OutputConfig

    @property
    def n_polls(self) -> int:
        """Number of scheduled ticks per feed for the configured duration."""
        return int((self.sampling.duration_minutes * 60) // self.sampling.poll_interval_seconds)


# Numeric fields that must be strictly positive, by section.
_POSITIVE_FIELDS = {
    "sampling": (
        "duration_minutes",
        "poll_interval_seconds",
        "request_timeout_seconds",
        "max_concurrent_requests",
        "flush_every_polls",
    ),
    "criteria": (
        "movement_threshold_meters",
        "max_update_gap_seconds",
        "stale_gap_seconds",
    ),
}

_SECTION_TYPES = {
    "sampling": SamplingConfig,
    "criteria": CriteriaConfig,
    "output": OutputConfig,
}


def _reject_unknown_keys(section: str, provided: dict[str, Any], dataclass_type: type) -> None:
    known = {f.name for f in dataclasses.fields(dataclass_type)}
    unknown = sorted(set(provided) - known)
    if unknown:
        raise ConfigError(f"unknown key(s) in '{section}': {unknown}. Known keys: {sorted(known)}")


def _require_positive(section: str, values: dict[str, Any]) -> None:
    for field in _POSITIVE_FIELDS.get(section, ()):
        if field not in values:
            continue
        value = values[field]
        if not isinstance(value, (int, float)) or isinstance(value, bool):
            raise ConfigError(f"{section}.{field} must be a number, got {value!r}")
        if math.isnan(value) or value <= 0:
            raise ConfigError(f"{section}.{field} must be > 0, got {value!r}")


def _parse_feeds(raw: Any) -> tuple[str, ...]:
    if not isinstance(raw, list) or not raw:
        raise ConfigError("'feeds' must be a non-empty list of warehouse feed names")
    names = []
    for item in raw:
        if not isinstance(item, str) or not item.strip():
            raise ConfigError(f"every entry in 'feeds' must be a non-empty string, got {item!r}")
        names.append(item.strip())
    duplicates = sorted({n for n in names if names.count(n) > 1})
    if duplicates:
        raise ConfigError(f"duplicate feed name(s) in 'feeds': {duplicates}")
    return tuple(names)


def _parse_output(raw: dict[str, Any]) -> OutputConfig:
    _reject_unknown_keys("output", raw, OutputConfig)
    kwargs: dict[str, Any] = dict(raw)
    if "dir" in kwargs:
        kwargs["dir"] = Path(str(kwargs["dir"])).expanduser()
    if "percentiles" in kwargs:
        pcts = kwargs["percentiles"]
        if not isinstance(pcts, list) or not pcts:
            raise ConfigError("output.percentiles must be a non-empty list")
        for p in pcts:
            if not isinstance(p, (int, float)) or isinstance(p, bool) or not 0 < p < 100:
                raise ConfigError(f"output.percentiles entries must be in (0, 100), got {p!r}")
        kwargs["percentiles"] = tuple(int(p) for p in pcts)
    if "write_raw_samples" in kwargs and not isinstance(kwargs["write_raw_samples"], bool):
        raise ConfigError("output.write_raw_samples must be a boolean")
    return OutputConfig(**kwargs)


def load_config(path: str | Path) -> tuple[SearchConfig, list[str]]:
    """
    Read and validate a YAML config.

    Returns the config plus a list of non-fatal warnings. The warnings are carried into the
    feed summary so a questionable run is legible from its output alone, not just its console.
    """
    path = Path(path)
    if not path.exists():
        raise ConfigError(f"config file not found: {path}")

    with path.open() as f:
        raw = yaml.safe_load(f)
    if not isinstance(raw, dict):
        raise ConfigError(f"config must be a YAML mapping, got {type(raw).__name__}")

    top_level_known = {"feeds", "sampling", "criteria", "output"}
    unknown_top = sorted(set(raw) - top_level_known)
    if unknown_top:
        raise ConfigError(f"unknown top-level key(s): {unknown_top}. Known keys: {sorted(top_level_known)}")

    for required in ("feeds", "sampling", "criteria"):
        if required not in raw:
            raise ConfigError(f"missing required section: '{required}'")

    feeds = _parse_feeds(raw["feeds"])

    sections: dict[str, Any] = {}
    for name in ("sampling", "criteria"):
        values = raw[name]
        if not isinstance(values, dict):
            raise ConfigError(f"'{name}' must be a mapping")
        _reject_unknown_keys(name, values, _SECTION_TYPES[name])
        _require_positive(name, values)
        sections[name] = values

    criteria_values = sections["criteria"]
    if "min_pass_fraction" in criteria_values:
        fraction = criteria_values["min_pass_fraction"]
        if not isinstance(fraction, (int, float)) or isinstance(fraction, bool):
            raise ConfigError(f"criteria.min_pass_fraction must be a number, got {fraction!r}")
        if not 0 < fraction <= 1:
            raise ConfigError(f"criteria.min_pass_fraction must be in (0, 1], got {fraction!r}")
    if "exclude_absence_gaps" in criteria_values and not isinstance(criteria_values["exclude_absence_gaps"], bool):
        raise ConfigError("criteria.exclude_absence_gaps must be a boolean")

    try:
        sampling = SamplingConfig(**sections["sampling"])
        criteria = CriteriaConfig(**criteria_values)
    except TypeError as exc:  # missing required field
        raise ConfigError(str(exc)) from exc

    output_raw = raw.get("output") or {}
    if not isinstance(output_raw, dict):
        raise ConfigError("'output' must be a mapping")
    output = _parse_output(output_raw)

    config = SearchConfig(feeds=feeds, sampling=sampling, criteria=criteria, output=output)

    warnings: list[str] = []
    # Observed gaps quantize to the poll rate, so polling slower than n/2 cannot resolve
    # whether real updates land under n. See also the resolution_limited flag in metrics.py.
    if sampling.poll_interval_seconds > criteria.max_update_gap_seconds / 2:
        warnings.append(
            f"poll_interval_seconds ({sampling.poll_interval_seconds:g}) is more than half of "
            f"max_update_gap_seconds ({criteria.max_update_gap_seconds:g}); observed gaps quantize "
            f"to the poll rate and 'frequent' may be unresolvable"
        )
    if config.n_polls < 2:
        warnings.append(
            f"duration_minutes ({sampling.duration_minutes:g}) yields {config.n_polls} poll(s); "
            f"no gaps can be measured"
        )

    return config, warnings
