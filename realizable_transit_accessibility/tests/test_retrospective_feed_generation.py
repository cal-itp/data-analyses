import pytest
from zipfile import ZipFile
import pathlib
import numpy as np
import pandas as pd
from gtfslite import GTFS
from retrospective_feed_generation import retrospective_feed_generation
from .constants import DEFAULT_TEST_FEED_GENERATION_KWARGS

@pytest.fixture
def parent(request):
    return request.path.parent

def test_feed_files(parent):
    """Test whether a feed created through retrospective accessibility have the same files"""
    # Enumerate the files to check for
    required_files = ["agency.txt", "stops.txt", "routes.txt", "trips.txt", "stop_times.txt"]
    extra_files = ["feed_info.txt", "transfers.txt", "calendar.txt", "calendar_dates.txt", "fare_attributes.txt", "fare_rules.txt", "frequencies.txt", "shapes.txt"]
    expected_files = required_files + extra_files
    schedule_path = parent / "test_data/test_no_change/big_blue_bus.zip"
    rt_path = parent / "test_data/test_no_change/big_blue_bus.csv"
    output_schedule_path = parent / "test_data/temp_data/test_feed_files.zip"
    
    def check_files_exist(path: pathlib.Path, files: list[str]) -> None:
        with ZipFile(path, "r") as z:
            found_files = z.namelist()
        intersect_files = np.intersect1d(files, found_files)
        assert len(intersect_files) == len(files), "At least one expected file is not present"
        assert len(intersect_files) == len(found_files), "At least one file is not expected"

    check_files_exist(schedule_path, expected_files)
    schedule_feed = GTFS.load_zip(schedule_path)
    rt_table = pd.read_csv(rt_path)
    output_feed = retrospective_feed_generation.make_retrospective_feed_single_date(
        filtered_input_feed=schedule_feed,
        stop_times_table=rt_table,
        **DEFAULT_TEST_FEED_GENERATION_KWARGS
    )
    output_feed.write_zip(output_schedule_path)
    check_files_exist(output_schedule_path, expected_files)