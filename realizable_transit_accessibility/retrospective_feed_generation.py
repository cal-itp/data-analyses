from gtfslite import GTFS
from gtfs_utils import copy_GTFS, time_string_to_time_since_midnight, seconds_to_gtfs_format_time
from constants import RT_COLUMN_RENAME_MAP
import pandas as pd
import numpy as np

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
        stop_times_merged_filtered["rt_arrival_sec"]
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