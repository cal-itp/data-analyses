"""Make the flat gtfs_rt_search modules importable from the tests."""

import sys
from pathlib import Path

PACKAGE_DIR = Path(__file__).resolve().parent.parent
FIXTURES_DIR = Path(__file__).resolve().parent / "fixtures"

if str(PACKAGE_DIR) not in sys.path:
    sys.path.insert(0, str(PACKAGE_DIR))
