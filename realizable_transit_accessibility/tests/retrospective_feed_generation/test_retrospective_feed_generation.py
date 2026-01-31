import pathlib

import numpy as np
import pandas as pd
import pytest
from gtfslite import GTFS
from retrospective_feed_generation import retrospective_feed_generation

from ..constants import (
    DEFAULT_TEST_FEED_GENERATION_KWARGS,
    EXTRA_COLUMNS_FEED_GENERATION_KWARGS,
    RT_ARRIVAL_SEC_NAME,
    STOP_SEQUENCE_NAME,
    TRIP_ID_NAME,
)


@pytest.fixture
def test_data_path(request):
    return request.path.parent.joinpath("..", "test_data")


def _gtfslite_from_folder(
    folder_path: pathlib.Path | str,
    alt_trips_path: pathlib.Path | str | None = None,
    alt_stop_times_path: pathlib.Path | str | None = None,
) -> GTFS:
    txt_files = pathlib.Path(folder_path).glob("*.txt")
    found_files = {path.stem: pd.read_csv(path) for path in txt_files}
    if alt_trips_path is not None:
        found_files["trips"] = pd.read_csv(alt_trips_path)
    if alt_stop_times_path is not None:
        found_files["stop_times"] = pd.read_csv(alt_stop_times_path)
    return GTFS(**found_files)


@pytest.fixture
def schedule_feed_minimal(test_data_path) -> GTFS:
    path = test_data_path.joinpath("minimal_schedule")
    return _gtfslite_from_folder(path)


@pytest.fixture
def two_trip_schedule(test_data_path) -> GTFS:
    return _gtfslite_from_folder(
        test_data_path.joinpath("minimal_schedule"),
        alt_trips_path=(test_data_path.joinpath("two_trip_schedule", "trips.txt")),
        alt_stop_times_path=(test_data_path.joinpath("two_trip_schedule", "stop_times.txt")),
    )


@pytest.fixture
def minimal_rt_table_schedule_rt_different(test_data_path) -> pd.DataFrame:
    path = test_data_path.joinpath("minimal_rt_table_schedule_rt_different.csv")
    return pd.read_csv(path)


@pytest.fixture
def two_trip_rt_table_schedule_rt_different(test_data_path) -> pd.DataFrame:
    path = test_data_path.joinpath("two_trip_rt_table_schedule_rt_different.csv")
    return pd.read_csv(path)


@pytest.fixture
def two_trip_rt_table_schedule_rt_same(test_data_path) -> pd.DataFrame:
    path = test_data_path.joinpath("two_trip_rt_table_schedule_rt_same.csv")
    return pd.read_csv(path)


@pytest.fixture
def expected_tables(test_data_path) -> list[str]:
    path = test_data_path.joinpath("minimal_schedule")
    tables = [file.stem for file in path.glob("*.txt")]
    return tables


@pytest.fixture
def minimal_rt_table_schedule_rt_different_schedule_arrival_altered(
    test_data_path,
) -> pd.DataFrame:
    path = test_data_path.joinpath("minimal_rt_table_schedule_rt_different_schedule_arrival_altered.csv")
    return pd.read_csv(path)


@pytest.fixture
def minimal_rt_table_schedule_rt_different_stop_id_altered(
    test_data_path,
) -> pd.DataFrame:
    path = test_data_path.joinpath("minimal_rt_table_schedule_rt_different_stop_id_altered.csv")
    return pd.read_csv(path)


@pytest.fixture
def two_trip_rt_table_schedule_one_trip_na(test_data_path) -> pd.DataFrame:
    path = test_data_path.joinpath("two_trip_rt_table_schedule_one_trip_na.csv")
    return pd.read_csv(path)


def test_feed_tables_exist(
    schedule_feed_minimal: GTFS,
    minimal_rt_table_schedule_rt_different: pd.DataFrame,
    expected_tables: list[str],
):
    """Test whether a feed has all tables from `filtered_input_feed`"""

    output_feed = retrospective_feed_generation.make_retrospective_feed_single_date(
        filtered_input_feed=schedule_feed_minimal,
        stop_times_table=minimal_rt_table_schedule_rt_different,
        **DEFAULT_TEST_FEED_GENERATION_KWARGS,
    )
    for table in expected_tables:
        expected_table = getattr(output_feed, table)
        assert expected_table is not None, f"{table} is not present"


def test_feed_tables_same(
    schedule_feed_minimal: GTFS,
    minimal_rt_table_schedule_rt_different: pd.DataFrame,
    expected_tables: list[str],
):
    """Test whether all tables from `filtered_input_feed` except stops.txt and stop_times.txt are unchanged in the output"""
    ignored_tables = ["stop_times", "stops"]
    output_feed = retrospective_feed_generation.make_retrospective_feed_single_date(
        filtered_input_feed=schedule_feed_minimal,
        stop_times_table=minimal_rt_table_schedule_rt_different,
        **DEFAULT_TEST_FEED_GENERATION_KWARGS,
    )
    for table in np.setdiff1d(expected_tables, ignored_tables):
        schedule_table = getattr(schedule_feed_minimal, table)
        output_table = getattr(output_feed, table)
        assert output_table.equals(schedule_table), f"{table} is not identical in the input and output"


# Removed this test, since this assertion seems to come up with normal data.
# If a switch is made to use only warehouse data, a similar test may be useful.
# def test_trips_present_in_rt_only_leads_to_exception(
#     schedule_feed_minimal: GTFS, two_trip_rt_table_schedule_rt_different: pd.DataFrame
# ):
#     """Test that, if there are trips in `stop_times_table` that are not in the schedule, a key error is raised"""
#     # Check setup conditions
#     assert (
#         schedule_feed_minimal.trips.trip_id == 1
#     ).all(), "Missing or extraneous trip ID in schedule feed (indicates bad test setup)"
#     assert two_trip_rt_table_schedule_rt_different.trip_id.isin(
#         [1, 2]
#     ).all(), "Missing or extraneous trip ID in RT table (indicates bad test setup)"

#     # Run feed generation
#     with pytest.raises(KeyError):
#         output_feed = retrospective_feed_generation.make_retrospective_feed_single_date(
#             filtered_input_feed=schedule_feed_minimal,
#             stop_times_table=two_trip_rt_table_schedule_rt_different,
#             **DEFAULT_TEST_FEED_GENERATION_KWARGS,
#         )


def test_trips_in_schedule_only_are_dropped_from_trips(
    two_trip_schedule: GTFS, minimal_rt_table_schedule_rt_different: pd.DataFrame
):
    """Test that trips that are only present in `filtered_input_feed` are dropped from the output trips table"""
    # Check setup conditions
    assert two_trip_schedule.trips.trip_id.isin(
        [1, 2]
    ).all(), "Missing or extraneous trip ID in RT table (indicates bad test setup)"
    assert (
        minimal_rt_table_schedule_rt_different.trip_id == 1
    ).all(), "Missing or extraneous trip ID in schedule feed (indicates bad test setup)"
    output_feed = retrospective_feed_generation.make_retrospective_feed_single_date(
        filtered_input_feed=two_trip_schedule,
        stop_times_table=minimal_rt_table_schedule_rt_different,
        **DEFAULT_TEST_FEED_GENERATION_KWARGS,
    )

    # Check that there aren't extra trips
    output_trip_ids = output_feed.trips.trip_id
    assert (
        output_trip_ids == 1
    ).all(), f'Trips with an ID other than "1" were present in the output trips table: {output_trip_ids.unique()}'


def test_trips_in_schedule_only_are_dropped_from_stop_times(
    two_trip_schedule: GTFS, minimal_rt_table_schedule_rt_different: pd.DataFrame
):
    # Test setup
    """Test that trips that are only present in `filtered_input_feed` are dropped from the output stop times table"""
    assert two_trip_schedule.stop_times.trip_id.isin(
        [1, 2]
    ).all(), "Missing or extraneous trip ID in RT table (indicates bad test setup)"
    assert (
        minimal_rt_table_schedule_rt_different.trip_id == 1
    ).all(), "Missing or extraneous trip ID in schedule feed (indicates bad test setup)"

    # Run feed generation
    output_feed = retrospective_feed_generation.make_retrospective_feed_single_date(
        filtered_input_feed=two_trip_schedule,
        stop_times_table=minimal_rt_table_schedule_rt_different,
        **DEFAULT_TEST_FEED_GENERATION_KWARGS,
    )

    # Check output
    # Check that there aren't extra trips
    output_stop_times_trip_ids = output_feed.stop_times.trip_id
    assert (
        output_stop_times_trip_ids == 1
    ).all(), f'Trips with an ID other than "1" were present in the output stop times table: {output_stop_times_trip_ids.unique()}'
    # Check that there are the correct number of records
    output_len = len(output_stop_times_trip_ids)
    input_rt_len = len(minimal_rt_table_schedule_rt_different.trip_id)
    assert (
        output_len == input_rt_len
    ), f"Output feed stop times has a different number of trip id values ({output_len}) than input RT table ({input_rt_len})"


def test_rt_stop_times_match_output_stop_times(
    two_trip_schedule: GTFS, two_trip_rt_table_schedule_rt_different: pd.DataFrame
):
    """Test that `arrival_time` and `departure_time` in the output `stop_times` match `stop_times_table` where `trip_id` and `stop_sequence` are equal"""
    # Run feed generation
    output_feed = retrospective_feed_generation.make_retrospective_feed_single_date(
        filtered_input_feed=two_trip_schedule,
        stop_times_table=two_trip_rt_table_schedule_rt_different,
        **DEFAULT_TEST_FEED_GENERATION_KWARGS,
    )

    # Check output
    # Check that both expected trips are present and there are no additional trip ids
    assert 1 in output_feed.stop_times.trip_id, "trip_id 1 is missing from the output feed"
    assert 2 in output_feed.stop_times.trip_id, "trip_id 2 is missing from the output feed"
    assert output_feed.stop_times.trip_id.isin(
        [1, 2]
    ).all(), f"A trip_id other than 1 or 2 is present in the ouutput feed. Unique trip_ids are: {output_feed.stop_times.trip_id.unique()}"
    # Merge output stop times and rt stop times by trip id and stop sequence
    merged_output_rt = output_feed.stop_times.merge(
        two_trip_rt_table_schedule_rt_different[[TRIP_ID_NAME, STOP_SEQUENCE_NAME, RT_ARRIVAL_SEC_NAME]],
        on=[TRIP_ID_NAME, STOP_SEQUENCE_NAME],
        how="outer",
        validate="one_to_one",
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
    assert merged_output_rt[RT_ARRIVAL_SEC_NAME].equals(
        merged_output_rt["output_arrival_time_seconds"]
    ), "Rt arrival sec does not match output arrival sec at same trip id and stop sequence"
    # Check that output departure time is the same as arrival time
    assert (
        merged_output_rt["arrival_time"] == merged_output_rt["departure_time"]
    ).all(), "Arrival times do not match departure times in output"


def test_no_mutability_issues(
    schedule_feed_minimal: GTFS,
    minimal_rt_table_schedule_rt_different: pd.DataFrame,
    expected_tables: list[str],
):
    """Test that all attributes in the output are not the same object as the input in `filtered_input_feed` (to ensure that there aren't mutability issues)"""
    ignored_tables = ["stop_times", "stops"]
    output_feed = retrospective_feed_generation.make_retrospective_feed_single_date(
        filtered_input_feed=schedule_feed_minimal,
        stop_times_table=minimal_rt_table_schedule_rt_different,
        **DEFAULT_TEST_FEED_GENERATION_KWARGS,
    )
    for table in np.setdiff1d(expected_tables, ignored_tables):
        output_table = getattr(output_feed, table)
        original_table = getattr(schedule_feed_minimal, table)
        assert (
            output_table is not original_table
        ), "make_retrospective_feed_single_date is returning the original update"


def test_validation_stop_ids(
    schedule_feed_minimal: GTFS,
    minimal_rt_table_schedule_rt_different_stop_id_altered: pd.DataFrame,
):
    """Test that if `validate=True` and `filtered_input_feed` and `stop_times_table` do not have matching stop ids at the same value of trip_id and stop_sequence, an `AssertionError` is raised"""
    with pytest.raises(AssertionError):
        output_feed = retrospective_feed_generation.make_retrospective_feed_single_date(
            filtered_input_feed=schedule_feed_minimal,
            stop_times_table=minimal_rt_table_schedule_rt_different_stop_id_altered,
            validate=True,
            **DEFAULT_TEST_FEED_GENERATION_KWARGS,
        )


def test_validation_schedule_times(
    schedule_feed_minimal: GTFS,
    minimal_rt_table_schedule_rt_different_schedule_arrival_altered: pd.DataFrame,
):
    """Test that if `validate=True` and `filtered_input_feed` and `stop_times_table` do not have matching schedule arrival times at the same value of trip_id and stop_sequence, an `AssertionError` is raised"""
    with pytest.raises(AssertionError):
        output_feed = retrospective_feed_generation.make_retrospective_feed_single_date(
            filtered_input_feed=schedule_feed_minimal,
            stop_times_table=minimal_rt_table_schedule_rt_different_schedule_arrival_altered,
            validate=True,
            **DEFAULT_TEST_FEED_GENERATION_KWARGS,
        )


def test_schedule_rt_equal_stop_times(two_trip_schedule: GTFS, two_trip_rt_table_schedule_rt_same: pd.DataFrame):
    """Test that, if the data in `filtered_input_feed` and `stop_times_table` represent identical trips, the `stop_times` table in the output feed is identical to the schedule version except that columns are as specified in `stop_times_desired_columns`"""
    # Get the the expected columns from the default args
    expected_columns = EXTRA_COLUMNS_FEED_GENERATION_KWARGS["stop_times_desired_columns"]

    # Run feed generation
    output_feed = retrospective_feed_generation.make_retrospective_feed_single_date(
        filtered_input_feed=two_trip_schedule,
        stop_times_table=two_trip_rt_table_schedule_rt_same,
        **EXTRA_COLUMNS_FEED_GENERATION_KWARGS,
    )

    # Get the stop times table
    schedule_table = two_trip_schedule.stop_times
    output_table = output_feed.stop_times
    # Check that the expected columns are present
    assert list(expected_columns) == list(output_table.columns), "Check that the output table has the specified columns"
    output_table_columns_selected = output_table[expected_columns].copy()
    schedule_table_columns_selected = schedule_table[expected_columns].copy()
    # Check that contents and order of stop times tables are maintained in the output
    assert schedule_table_columns_selected.equals(
        output_table_columns_selected
    ), "stop_times is not identical between the schedule input and output"


def test_filter_non_rt_trips(two_trip_rt_table_schedule_one_trip_na: pd.DataFrame):
    """Test that, if a trip has all na stop times, retrospective_feed_generation.filter_non_rt_trips will remove it from the DF"""
    filtered = retrospective_feed_generation.filter_non_rt_trips(two_trip_rt_table_schedule_one_trip_na)
    assert (filtered.trip_instance_key == "key2").all(), "Unexpected trip found in filtered output"
