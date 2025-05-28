from gtfslite import GTFS
from gtfs_utils import copy_GTFS, time_string_to_time_since_midnight, seconds_to_gtfs_format_time
from constants import RT_COLUMN_RENAME_MAP
import pandas as pd
import numpy as np

def flag_nonsequential_stops(rt_schedule_stop_times_sorted: pd.DataFrame) -> pd.DataFrame:
    grouped_by_trip = rt_schedule_stop_times_sorted.groupby(
        ["schedule_gtfs_dataset_key", "trip_instance_key"]
    )
    shifted_grouped = grouped_by_trip[["scheduled_arrival_sec", "rt_arrival_sec"]].shift(1)
    df_output = rt_schedule_stop_times_sorted.copy()
    df_output["non_sequential_rt_arrival"] = (
        shifted_grouped["rt_arrival_sec"] > df_output["rt_arrival_sec"]
    ).copy()
    df_output["flag_surrounding_non_sequential_rt_arrival"] = (
        df_output["non_sequential_rt_arrival"] | df_output["non_sequential_rt_arrival"].shift(-1)
    ).copy()
    return df_output

def impute_first_last(rt_schedule_stop_times_sorted: pd.DataFrame) -> pd.DataFrame:
    assert not rt_schedule_stop_times_sorted["scheduled_arrival_sec"].isna().any()
    # Get the first & last stop time in each trip
    stop_time_grouped = rt_schedule_stop_times_sorted.groupby("trip_instance_key")
    first_stop_time = stop_time_grouped.first()
    first_stop_sequence = first_stop_time["stop_sequence"].rename("first_stop_sequence")
    last_stop_time = stop_time_grouped.last()
    last_stop_sequence = last_stop_time["stop_sequence"].rename("last_stop_sequence")
    # Get the first / last stop time with RT data that is not the first/last stop time overall (resp.)
    # We need this to have a baseline to impute the first/last stop times
    stop_times_with_first_last_sequence = rt_schedule_stop_times_sorted.merge(
        pd.concat([first_stop_sequence, last_stop_sequence], axis=1),
        on='trip_instance_key',
        how='left',
        validate="many_to_one"
    )
    stop_times_na_dropped = stop_times_with_first_last_sequence.loc[
        stop_times_with_first_last_sequence['rt_arrival_sec'].notna() &
        ~stop_times_with_first_last_sequence["flag_surrounding_non_sequential_rt_arrival"]
    ]
    # Get the "second" stop time
    second_candidates = stop_times_na_dropped[
        stop_times_na_dropped['stop_sequence'] > stop_times_na_dropped['first_stop_sequence']
    ]
    second_stop_time = second_candidates.groupby(
        'trip_instance_key'
    ).first()
    # Get the "penultimate" stop time
    penultimate_candidates = stop_times_na_dropped[
        stop_times_na_dropped["stop_sequence"] < stop_times_na_dropped["last_stop_sequence"]
    ]
    penultimate_stop_time = penultimate_candidates.groupby(
        'trip_instance_key'
    ).last()
    # Get the scheduled time between first & "second" and "penultimate" & last stop
    scheduled_first_second_difference = second_stop_time["scheduled_arrival_sec"] - first_stop_time["scheduled_arrival_sec"]
    scheduled_penultimate_last_difference = last_stop_time["scheduled_arrival_sec"] - penultimate_stop_time["scheduled_arrival_sec"]

    assert (scheduled_first_second_difference.isna() |(scheduled_first_second_difference > 0)).all()
    assert (scheduled_penultimate_last_difference.isna() |(scheduled_penultimate_last_difference > 0)).all()
    rt_first_imputed = (
        second_stop_time["rt_arrival_sec"] - scheduled_first_second_difference
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
         "last_stop_sequence"
    ], axis=1)

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