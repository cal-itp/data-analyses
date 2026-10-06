import textwrap

import pytest
from config import ConfigError, load_config

VALID = """
feeds:
  - VCTC Vehicle Positions
sampling:
  duration_minutes: 30
  poll_interval_seconds: 5
criteria:
  movement_threshold_meters: 10
  max_update_gap_seconds: 20
"""


def write(tmp_path, body):
    path = tmp_path / "config.yml"
    path.write_text(textwrap.dedent(body))
    return path


def test_loads_valid_config(tmp_path):
    config, warnings = load_config(write(tmp_path, VALID))
    assert config.feeds == ("VCTC Vehicle Positions",)
    assert config.sampling.poll_interval_seconds == 5
    assert config.criteria.min_pass_fraction == 0.9
    assert config.n_polls == 360
    assert warnings == []


def test_rejects_unknown_top_level_key(tmp_path):
    with pytest.raises(ConfigError, match="unknown top-level key"):
        load_config(write(tmp_path, VALID + "\nextra: 1\n"))


def test_rejects_unknown_section_key(tmp_path):
    body = VALID.replace("  poll_interval_seconds: 5", "  poll_interval_seconds: 5\n  poll_intervall_seconds: 5")
    with pytest.raises(ConfigError, match="unknown key"):
        load_config(write(tmp_path, body))


def test_rejects_zero_interval(tmp_path):
    body = VALID.replace("poll_interval_seconds: 5", "poll_interval_seconds: 0")
    with pytest.raises(ConfigError, match="must be > 0"):
        load_config(write(tmp_path, body))


@pytest.mark.parametrize("fraction", ["0", "1.5", "-0.2"])
def test_rejects_bad_pass_fraction(tmp_path, fraction):
    body = VALID + f"  min_pass_fraction: {fraction}\n"
    with pytest.raises(ConfigError, match="min_pass_fraction"):
        load_config(write(tmp_path, body))


def test_accepts_pass_fraction_of_one(tmp_path):
    config, _ = load_config(write(tmp_path, VALID + "  min_pass_fraction: 1.0\n"))
    assert config.criteria.min_pass_fraction == 1.0


def test_rejects_duplicate_feeds(tmp_path):
    body = VALID.replace(
        "  - VCTC Vehicle Positions",
        "  - VCTC Vehicle Positions\n  - VCTC Vehicle Positions",
    )
    with pytest.raises(ConfigError, match="duplicate feed name"):
        load_config(write(tmp_path, body))


def test_rejects_empty_feeds(tmp_path):
    with pytest.raises(ConfigError, match="non-empty list"):
        load_config(write(tmp_path, VALID.replace("  - VCTC Vehicle Positions", "")))


def test_warns_when_polling_slower_than_half_n(tmp_path):
    # 15s polling cannot resolve whether real updates land inside 20s.
    body = VALID.replace("poll_interval_seconds: 5", "poll_interval_seconds: 15")
    config, warnings = load_config(write(tmp_path, body))
    assert config.sampling.poll_interval_seconds == 15
    assert any("quantize" in w for w in warnings)


def test_no_warning_at_exactly_half_n(tmp_path):
    body = VALID.replace("poll_interval_seconds: 5", "poll_interval_seconds: 10")
    _, warnings = load_config(write(tmp_path, body))
    assert not any("quantize" in w for w in warnings)


def test_missing_file(tmp_path):
    with pytest.raises(ConfigError, match="not found"):
        load_config(tmp_path / "nope.yml")
