import pytest
import pathlib
import pandas as pd
import numpy as np
from gtfslite import GTFS
from retrospective_feed_generation import retrospective_feed_generation
from .constants import DEFAULT_TEST_FEED_GENERATION_KWARGS

def _get_test_data_path(request):
    return request.path.parent / "test_data"

def _gtfslite_from_folder(path: pathlib.Path | str) -> GTFS:
    txt_files = pathlib.Path(path).glob("*.txt")
    found_files = {
        path.stem: pd.read_csv(path) for path in txt_files
    }
    print(found_files.keys())
    return GTFS(**found_files)
    
@pytest.fixture
def schedule_feed(request) -> GTFS:
    path = _get_test_data_path(request) / "test_no_change" / "minimal_schedule"
    print(path)
    return _gtfslite_from_folder(path)

@pytest.fixture
def rt_table(request) -> pd.DataFrame:
    path = _get_test_data_path(request) / "test_no_change" / "minimal_rt_table.csv"
    print(path)
    return pd.read_csv(path)

@pytest.fixture
def expected_tables(request) -> list[str]:
    path = _get_test_data_path(request) / "test_no_change" / "minimal_schedule"
    tables = [file.stem for file in path.glob("*.txt")]
    return tables

def test_feed_tables_exist(schedule_feed: GTFS, rt_table: pd.DataFrame, expected_tables: list[str]):
    """Test whether a feed created through retrospective accessibility has all expected tables"""
    
    output_feed = retrospective_feed_generation.make_retrospective_feed_single_date(
        filtered_input_feed=schedule_feed,
        stop_times_table=rt_table,
        **DEFAULT_TEST_FEED_GENERATION_KWARGS
    )
    for table in expected_tables:
        expected_table = getattr(output_feed, table)
        assert expected_table is not None, f"{table} is not present"

def test_feed_tables_same(schedule_feed: GTFS, rt_table: pd.DataFrame, expected_tables: list[str]):
    """Test whether all tables except stops.txt and stop_times.txt are unchanged in the output"""
    ignored_tables = ["stop_times", "stops"]
    output_feed = retrospective_feed_generation.make_retrospective_feed_single_date(
        filtered_input_feed=schedule_feed,
        stop_times_table=rt_table,
        **DEFAULT_TEST_FEED_GENERATION_KWARGS
    )
    for table in np.setdiff1d(expected_tables, ignored_tables):
        schedule_table = getattr(schedule_feed, table)
        output_table = getattr(output_feed, table)
        print("schedule",schedule_table)
        print("output",output_table)
        assert output_table.equals(schedule_table), f"{table} is not identical in the input andoutput"
    
    