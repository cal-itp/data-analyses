import pytest
import pathlib
import pandas as pd
import numpy as np
from gtfslite import GTFS
from retrospective_feed_generation import retrospective_feed_generation
from .constants import DEFAULT_TEST_FEED_GENERATION_KWARGS, STOP_SEQUENCE_NAME, TRIP_ID_NAME, RT_ARRIVAL_SEC_NAME

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
def schedule_feed_minimal(request) -> GTFS:
    path = _get_test_data_path(request) / "minimal_schedule"
    return _gtfslite_from_folder(path)

@pytest.fixture
def two_trip_schedule(request) -> GTFS:
    path = _get_test_data_path(request) / "two_trip_schedule"
    return _gtfslite_from_folder(path)

@pytest.fixture
def minimal_rt_table_schedule_rt_different(request) -> pd.DataFrame:
    path = _get_test_data_path(request) / "minimal_rt_table_schedule_rt_different.csv"
    return pd.read_csv(path)

@pytest.fixture
def two_trip_rt_table_schedule_rt_different(request) -> pd.DataFrame:
    path = _get_test_data_path(request) / "two_trip_rt_table_schedule_rt_different.csv"
    return pd.read_csv(path)

@pytest.fixture
def expected_tables(request) -> list[str]:
    path = _get_test_data_path(request) / "minimal_schedule"
    tables = [file.stem for file in path.glob("*.txt")]
    return tables

def test_feed_tables_exist(schedule_feed_minimal: GTFS, minimal_rt_table_schedule_rt_different: pd.DataFrame, expected_tables: list[str]):
    """Test whether a feed created through retrospective accessibility has all expected tables"""
    
    output_feed = retrospective_feed_generation.make_retrospective_feed_single_date(
        filtered_input_feed=schedule_feed_minimal,
        stop_times_table=minimal_rt_table_schedule_rt_different,
        **DEFAULT_TEST_FEED_GENERATION_KWARGS
    )
    for table in expected_tables:
        expected_table = getattr(output_feed, table)
        assert expected_table is not None, f"{table} is not present"

def test_feed_tables_same(schedule_feed_minimal: GTFS, minimal_rt_table_schedule_rt_different: pd.DataFrame, expected_tables: list[str]):
    """Test whether all tables except stops.txt and stop_times.txt are unchanged in the output"""
    ignored_tables = ["stop_times", "stops"]
    output_feed = retrospective_feed_generation.make_retrospective_feed_single_date(
        filtered_input_feed=schedule_feed_minimal,
        stop_times_table=minimal_rt_table_schedule_rt_different,
        **DEFAULT_TEST_FEED_GENERATION_KWARGS
    )
    for table in np.setdiff1d(expected_tables, ignored_tables):
        schedule_table = getattr(schedule_feed_minimal, table)
        output_table = getattr(output_feed, table)
        print("schedule",schedule_table)
        print("output",output_table)
        assert output_table.equals(schedule_table), f"{table} is not identical in the input and output"

def test_trips_present_in_rt_only_leads_to_exception(schedule_feed_minimal: GTFS, two_trip_rt_table_schedule_rt_different: pd.DataFrame):
    """Check that, if there are trips in the rt table that are not in the schedule, a key error is raised"""
    # Check setup conditions
    assert (schedule_feed_minimal.trips.trip_id == 1).all(), "Missing or extraneous trip ID in schedule feed (inficates bad test setup)"
    assert two_trip_rt_table_schedule_rt_different.trip_id.isin([1, 2]).all(), "Missing or extraneous trip ID in RT table (indicates bad test setup)"
    
    # Run feed generation
    with pytest.raises(KeyError):
        output_feed = retrospective_feed_generation.make_retrospective_feed_single_date(
            filtered_input_feed=schedule_feed_minimal,
            stop_times_table=two_trip_rt_table_schedule_rt_different,
            **DEFAULT_TEST_FEED_GENERATION_KWARGS
        )
    
def test_trips_in_schedule_only_are_dropped_from_trips(two_trip_schedule: GTFS, minimal_rt_table_schedule_rt_different: pd.DataFrame):
    """Test that trips that are only present in the schedule feed are dropped from the output trips table"""
    # Check setup conditions
    assert two_trip_schedule.trips.trip_id.isin([1, 2]).all(), "Missing or extraneous trip ID in RT table (indicates bad test setup)"
    assert (minimal_rt_table_schedule_rt_different.trip_id == 1).all(), "Missing or extraneous trip ID in schedule feed (inficates bad test setup)"
    output_feed = retrospective_feed_generation.make_retrospective_feed_single_date(
        filtered_input_feed=two_trip_schedule,
        stop_times_table=minimal_rt_table_schedule_rt_different,
        **DEFAULT_TEST_FEED_GENERATION_KWARGS
    )
    
    # Check that there aren't extra trips
    output_trip_ids = output_feed.trips.trip_id
    assert (output_trip_ids == 1).all(), f"Trips with an ID other than \"1\" were present in the output trips table: {output_trip_ids.unique()}"

def test_trips_in_schedule_only_are_dropped_from_stop_times(two_trip_schedule: GTFS, minimal_rt_table_schedule_rt_different: pd.DataFrame):
    # Test setup
    """Test that trips that are only present in the schedule feed are dropped from the output stop times table"""
    assert two_trip_schedule.stop_times.trip_id.isin([1, 2]).all(), "Missing or extraneous trip ID in RT table (indicates bad test setup)"
    assert (minimal_rt_table_schedule_rt_different.trip_id == 1).all(), "Missing or extraneous trip ID in schedule feed (inficates bad test setup)"
    
    # Run feed generation
    output_feed = retrospective_feed_generation.make_retrospective_feed_single_date(
        filtered_input_feed=two_trip_schedule,
        stop_times_table=minimal_rt_table_schedule_rt_different,
        **DEFAULT_TEST_FEED_GENERATION_KWARGS
    )
    
    # Check output
    # Check that there aren't extra trips
    output_stop_times_trip_ids = output_feed.stop_times.trip_id
    assert (output_stop_times_trip_ids == 1).all(), f"Trips with an ID other than \"1\" were present in the output stop times table: {output_stop_times_trip_ids.unique()}"
    # Check that there are the correct number of records
    output_len = len(output_stop_times_trip_ids)
    input_rt_len = len(minimal_rt_table_schedule_rt_different.trip_id)
    assert output_len == input_rt_len, f"Output feed stop times has a different number of trip id values ({output_len}) than input RT table ({input_rt_len})"
    
def test_rt_stop_times_match_output_stop_times(two_trip_schedule: GTFS, two_trip_rt_table_schedule_rt_different: pd.DataFrame):
    """Test that arrival_time and departure_time in the output stop times table match rt stop times at matching trip id and stop sequence"""
    # Run feed generation
    output_feed = retrospective_feed_generation.make_retrospective_feed_single_date(
        filtered_input_feed=two_trip_schedule,
        stop_times_table=two_trip_rt_table_schedule_rt_different,
        **DEFAULT_TEST_FEED_GENERATION_KWARGS
    )
    
    # Check output
    # Check that both expected trips are present and there are no new trip ids
    assert 1 in output_feed.stop_times.trip_id
    assert 2 in output_feed.stop_times.trip_id
    assert output_feed.stop_times.trip_id.isin([1,2]).all()
    # Merge output stop times and rt stop times by trip id and stop sequence
    merged_output_rt = output_feed.stop_times.merge(
        two_trip_rt_table_schedule_rt_different[[TRIP_ID_NAME, STOP_SEQUENCE_NAME, RT_ARRIVAL_SEC_NAME]],
        on=[TRIP_ID_NAME, STOP_SEQUENCE_NAME],
        how="outer",
        validate="one_to_one"
    )
    def gtfs_time_to_seconds(gtfs_time_series: pd.Series) -> pd.Series:
        split_series = gtfs_time_series.str.split(":")
        # not the best way to do this, but simple!
        hours = split_series.map(lambda x: x[0]).astype(int)
        minutes = split_series.map(lambda x: x[1]).astype(int)
        seconds = split_series.map(lambda x: x[2]).astype(int)
        return (hours * 3600) + (minutes * 60) + seconds
    merged_output_rt["output_arrival_time_seconds"] = gtfs_time_to_seconds(merged_output_rt["arrival_time"])
    
    # Check that output times match
    assert merged_output_rt[RT_ARRIVAL_SEC_NAME].equals(merged_output_rt["output_arrival_time_seconds"]), "Rt arrival sec does not match output arrival sec at same trip id and stop sequence"
    # Check that output departure time is the same as arrival time
    assert (merged_output_rt["arrival_time"] == merged_output_rt["departure_time"]).all(), "Arrival times do not match departure times in output"
    
#TODO need to check that other columns are maintained in this scenario
    
def test_no_mutability_issues(schedule_feed_minimal: GTFS, minimal_rt_table_schedule_rt_different: pd.DataFrame, expected_tables: list[str]):
    """Check that altering non-schedule tables in the output feed does not alter the input feed"""
    ignored_tables = ["stop_times", "stops"]
    output_feed = retrospective_feed_generation.make_retrospective_feed_single_date(
        filtered_input_feed=schedule_feed_minimal,
        stop_times_table=minimal_rt_table_schedule_rt_different,
        **DEFAULT_TEST_FEED_GENERATION_KWARGS
    )
    for table in np.setdiff1d(expected_tables, ignored_tables):
        output_table = getattr(output_feed, table)
        original_table = getattr(schedule_feed_minimal, table)
        assert output_table is not original_table, "make_retrospective_feed_single_date is returning the original update"
        

    
#TODO: there should also be tests to check that the validate=True options work as expected
    