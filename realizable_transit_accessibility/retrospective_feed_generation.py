from gtfslite import GTFS
from gtfs_utils import copy_GTFS, time_string_to_time_since_midnight, seconds_to_gtfs_format_time
from constants import RT_COLUMN_RENAME_MAP
import pandas as pd
import numpy as np
import typing

ColumnId = str
ColumnName = typing.Hashable 
ColumnMap = dict[ColumnId, ColumnName]

def flag_nonmonotonic_sections(
        rt_schedule_stop_times_sorted: pd.DataFrame,
        trip_id_column: ColumnName, 
        rt_column: ColumnName, 
        schedule_column: ColumnName,
        **_unused_column_names: ColumnMap
) -> pd.DataFrame:
    rt_arrival_sec_shifted = rt_schedule_stop_times_sorted.groupby(
        trip_id_column
    )[rt_column].shift(1)
    rt_arrival_sec_dips = (
        (rt_arrival_sec_shifted > rt_schedule_stop_times_sorted[rt_column])
        & rt_schedule_stop_times_sorted[rt_column].notna()
    )
    print(rt_arrival_sec_dips.any())
    return rt_arrival_sec_dips

def add_monotonic_flag_to_df(
        rt_schedule_stop_times_sorted: pd.DataFrame, 
        **column_name_args: ColumnMap
) -> pd.DataFrame:
    df_output = rt_schedule_stop_times_sorted.copy()
    df_output["non_sequential_rt_arrival"] = flag_nonmonotonic_sections(
        rt_schedule_stop_times_sorted, **column_name_args
    )
    df_output["flag_surrounding_non_sequential_rt_arrival"] = (
        df_output["non_sequential_rt_arrival"] | df_output["non_sequential_rt_arrival"].shift(-1)
    ).copy()
    return df_output

def impute_first_last(
        rt_schedule_stop_times_sorted: pd.DataFrame, 
        trip_id_column: ColumnName,
        rt_column: ColumnName,
        schedule_column: ColumnName,
        stop_sequence_column: ColumnName,
        **_unused_column_name_args: ColumnMap
) -> pd.DataFrame:
    assert not rt_schedule_stop_times_sorted[schedule_column].isna().any()
    # Get the first & last stop time in each trip
    stop_time_grouped = rt_schedule_stop_times_sorted.groupby(trip_id_column)
    first_stop_time = stop_time_grouped.first()
    first_stop_sequence = first_stop_time[stop_sequence_column].rename("first_stop_sequence")
    last_stop_time = stop_time_grouped.last()
    last_stop_sequence = last_stop_time[stop_sequence_column].rename("last_stop_sequence")
    # Get the first / last stop time with RT data that is not the first/last stop time overall (resp.)
    # We need this to have a baseline to impute the first/last stop times
    stop_times_with_first_last_sequence = rt_schedule_stop_times_sorted.merge(
        pd.concat([first_stop_sequence, last_stop_sequence], axis=1),
        on=trip_id_column,
        how="left",
        validate="many_to_one"
    )
    stop_times_na_dropped = stop_times_with_first_last_sequence.loc[
        stop_times_with_first_last_sequence[rt_column].notna() &
        ~stop_times_with_first_last_sequence["flag_surrounding_non_sequential_rt_arrival"]
    ]
    # Get the "second" stop time
    second_candidates = stop_times_na_dropped[
        stop_times_na_dropped[stop_sequence_column] > stop_times_na_dropped['first_stop_sequence']
    ]
    second_stop_time = second_candidates.groupby(
        trip_id_column
    ).first()
    # Get the "penultimate" stop time
    penultimate_candidates = stop_times_na_dropped[
        stop_times_na_dropped[stop_sequence_column] < stop_times_na_dropped["last_stop_sequence"]
    ]
    penultimate_stop_time = penultimate_candidates.groupby(trip_id_column).last()
    # Get the scheduled time between first & "second" and "penultimate" & last stop
    scheduled_first_second_difference = second_stop_time[schedule_column] - first_stop_time[schedule_column]
    scheduled_penultimate_last_difference = last_stop_time[schedule_column] - penultimate_stop_time[schedule_column]

    assert (scheduled_first_second_difference.isna() |(scheduled_first_second_difference > 0)).all()
    assert (scheduled_penultimate_last_difference.isna() |(scheduled_penultimate_last_difference > 0)).all()
    rt_first_imputed = (
        second_stop_time[rt_column] - scheduled_first_second_difference
    ).rename("first_arrival_sec_imputed")
    rt_last_imputed = (
        penultimate_stop_time["rt_arrival_sec"] + scheduled_penultimate_last_difference
    ).rename("last_arrival_sec_imputed")
    # Merge in imputed first times
    stop_times_imputed_merged = stop_times_with_first_last_sequence.merge(
        pd.concat([rt_first_imputed, rt_last_imputed], axis=1),
        how="left",
        left_on="trip_instance_key",
        right_index=True,
        validate="many_to_one"
    )
    # Combine imputed and rt columns
    stop_times_imputed_merged["imputed_arrival_sec"] = (
        stop_times_imputed_merged["rt_arrival_sec"].where(
            (
                stop_times_imputed_merged["first_stop_sequence"]
                != stop_times_imputed_merged["stop_sequence"]
            ),
            stop_times_imputed_merged["first_arrival_sec_imputed"]
        ).where(
            (
                stop_times_with_first_last_sequence["last_stop_sequence"] 
                != stop_times_with_first_last_sequence["stop_sequence"]
            ),
            stop_times_imputed_merged["last_arrival_sec_imputed"]
        )
    )
    return stop_times_imputed_merged.drop([
        "first_arrival_sec_imputed",
         "last_arrival_sec_imputed", 
         "first_stop_sequence", 
         "last_stop_sequence",
    ], axis=1)


def impute_non_monotonic_rt_times(rt_schedule_stop_times_sorted, rt_column):
    # Check that first/last trip times are imputed
    trip_id_grouped = rt_schedule_stop_times_sorted.groupby(trip_id_column)
    assert not trip_id_grouped[rt_column].first().isna().any()
    # Check that schedule values are present for all trips
    assert not rt_schedule_stop_times_sorted["scheduled_arrival_sec"].isna().any()
    # Check that the first and last values of each trip are not marked as nonmonotonic
    assert not trip_id_grouped["flag_surrounding_nonsequential_rt_arrival"].first().any()
    assert not trip_id_grouped["flag_surrounding_nonsequential_rt_arrival"].last().any()
    
    grouped_flag = rt_schedule_stop_times_sorted.groupby(
        "trip_instance_key"
    )[
        "flag_surrounding_non_sequential_rt_arrival"
    ]
    before_nonmonotonic = (
        grouped_flag.shift(-1)
        & ~rt_schedule_stop_times_sorted["flag_surrounding_non_sequential_rt_arrival"]
    )
    after_nonmonotonic = (
        grouped_flag.shift(1)
        & ~rt_schedule_stop_times_sorted["flag_surrounding_non_sequential_rt_arrival"]
    )
    # Get the schedule time at the last instance of before_nonmonotonic
    before_time_schedule = rt_schedule_stop_times_sorted.loc[
        before_nonmonotonic, "scheduled_arrival_sec"
    ].reindex(rt_schedule_stop_times_sorted.index, method="ffill")
    # Get the rt time at the last instance of before_nonmonotonic
    before_time_rt = rt_schedule_stop_times_sorted.loc[
        before_nonmonotonic, rt_column
    ].reindex(rt_schedule_stop_times_sorted.index, method="ffill")
    # Get the scheduled time at the next instance of after_nonmonotonic
    after_time_scheduled = rt_schedule_stop_times_sorted.loc[
        after_nonmonotonic, "scheduled_arrival_sec"
    ].reindex(rt_schedule_stop_times_sorted.index, method="bfill")
    # Get the difference between the current rt time and the next scheduled time
    imputed_difference = after_time_schedled - before_time_scheduled
    imputed_only_time = imputed_difference + before_time_rt
    merged_imputed_time = rt_schedule_stop_times_sorted[rt_column].where(
        ~rt_schedule_stop_times_sorted["flag_surrounding_non_sequential_rt_arrival"].,
        imputed_only_time
    )
    return merged_imputed_time
    
def make_retrospective_feed_single_date(
        filtered_input_feed: GTFS, 
        stop_times_table: pd.DataFrame,
        stop_times_desired_columns: list[str],
        validate: bool = True
) -> GTFS:
    schedule_trips_original = filtered_input_feed.trips.set_index("trip_id")
    schedule_stop_times_original = filtered_input_feed.stop_times.copy()
    schedule_stop_times_original["feed_arrival_sec"] = time_string_to_time_since_midnight(
        schedule_stop_times_original["arrival_time"]
    )
    rt_trip_ids = stop_times_table["trip_id"].drop_duplicates(keep="first")

    schedule_trips_in_rt = schedule_trips_original.loc[rt_trip_ids]
    stop_times_merged = schedule_stop_times_original.merge(
        stop_times_table.rename(
            columns=RT_COLUMN_RENAME_MAP
        ),
        on=["trip_id", "stop_sequence"],
        how="left", #TODO: left for proof of concept to simplify, should be outer
        validate="one_to_one"
    )
    
    if validate:
        # Validation
        # Stop ids match or are na
        assert (
            (stop_times_merged["stop_id"] == stop_times_merged["warehouse_stop_id"])
            | stop_times_merged["warehouse_stop_id"].isna()
        ).all()
        # Departure / arrival times match or are na
        assert (
            (stop_times_merged["feed_arrival_sec"] == stop_times_merged["warehouse_scheduled_arrival_sec"])
            | stop_times_merged["feed_arrival_sec"].isna()
            | stop_times_merged["warehouse_scheduled_arrival_sec"].isna()
        ).all()
        # All RT stop times have an arrival sec
        assert (
            ~stop_times_merged["feed_arrival_sec"].isna()
            | stop_times_merged["schedule_gtfs_dataset_key"].isna()
        ).all()
    
    stop_times_merged_filtered = stop_times_merged.loc[
        ~stop_times_merged["schedule_gtfs_dataset_key"].isna()
    ].reset_index(drop=True)
    stop_times_merged_filtered["rt_arrival_gtfs_time"] = seconds_to_gtfs_format_time(
        stop_times_merged_filtered["imputed_arrival_sec"]
    )
    stop_times_gtfs_format_with_rt_times = stop_times_merged_filtered.drop(
        ["arrival_time", "departure_time"], axis=1
    ).rename(
        columns={
            "rt_arrival_gtfs_time": "arrival_time",
        }
    )[
        np.intersect1d(
            stop_times_desired_columns,
            stop_times_merged_filtered.columns
        )
    ].copy()
    # TODO: not sure if this is the correct thing to do, for first/last trips
    #TODO: move this earlier on, so departure_time ends up in the desired position in columns
    stop_times_gtfs_format_with_rt_times["departure_time"] = (
        stop_times_gtfs_format_with_rt_times["arrival_time"].copy()
    )
        
    # Output a new synthetic feed!
    # Alter the feed with the new trips and stop times
    altered_feed = copy_GTFS(filtered_input_feed)
    altered_feed.trips = schedule_trips_in_rt.reset_index()
    altered_feed.stop_times = stop_times_gtfs_format_with_rt_times

    # Not sure if this is appropriate or not, since we're altering. Leaving commented out for now
    # Possibly should go in subset_schedule_feed_to_one_date
    """
    new_feed_info = pd.DataFrame({
        "feed_publisher_name": "California Department of Transportation",
        "feed_publisher_url": "https://dot.ca.gov",
        "feed_lang": np.nan if altered_feed.feed_info is not None else altered_feed.feed_info["feed_lang"].iloc[0],
        "feed_start_date": SAMPLE_DATE_STR,
        "feed_end_date": SAMPLE_DATE_STR,
        "feed_version": f"retrospective_{SAMPLE_DATE_STR}" if altered_feed.feed_info is not None else  f"retrospective_{altered_feed.feed_info["feed_version"]}_{SAMPLE_DATE_STR}"
    })
    """
    # Copy the feed - this is necessary to validate the feed meets the standard since gtfs-lite only validates feeds on creation
    output_feed = copy_GTFS(altered_feed)
    return output_feed