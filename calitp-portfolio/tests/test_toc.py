import os
from pathlib import Path

import pytest
import yaml

from calitp_portfolio.models import Site

SITE_FIXTURES = Path(__file__).parent / "fixtures" / "sites"
SNAPSHOTS = Path(__file__).parent / "snapshots"
UPDATE_SNAPSHOTS = os.environ.get("UPDATE_SNAPSHOTS") == "1"

ALL_TEST_YMLS = [
    "_basic_analyses_test.yml",
    "_group_analyses_test.yml",
    "_group_and_params_analyses_test.yml",
    "_param_analyses_test.yml",
    "_param_manual_title_analyses_test.yml",
    "_readme_only_analyses_test.yml",
    "_section_analyses_test.yml",
]


def _load_site(yml_name: str) -> Site:
    path = SITE_FIXTURES / yml_name
    name = path.stem
    with open(path) as f:
        return Site(output_dir=Path("./portfolio") / name, name=name, **yaml.safe_load(f))


def _check_snapshot(actual: str, snapshot_name: str) -> None:
    snapshot_path = SNAPSHOTS / snapshot_name
    if UPDATE_SNAPSHOTS or not snapshot_path.exists():
        snapshot_path.parent.mkdir(parents=True, exist_ok=True)
        snapshot_path.write_text(actual)
        return
    expected = snapshot_path.read_text()
    assert actual == expected, f"TOC output does not match snapshot {snapshot_name}"


@pytest.mark.parametrize("yml_name", ALL_TEST_YMLS)
def test_toc_yaml_matches_snapshot(yml_name):
    site = _load_site(yml_name)
    snapshot_name = f"toc_{yml_name.lstrip('_').replace('.yml', '.yml')}"
    _check_snapshot(site.toc_yaml, snapshot_name)
