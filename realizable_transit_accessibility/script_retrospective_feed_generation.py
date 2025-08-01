import datetime as dt
import pathlib

import columns as col
import geopandas as gpd
import numpy as np
import pandas as pd
from gtfs_utils import *
#  pip install gtfs-lite
from gtfslite import GTFS
from retrospective_feed_generation import *
from retrospective_feed_generation import _filter_na_stop_times, _filter_non_rt_trips
from shared_utils import catalog_utils, gtfs_utils_v2, rt_dates
from warehouse_utils import *
import argparse

def process_table_row(rt_date: str, schedule_local_path: str, schedule_name: str, output_local_name: str, max_stop_gap: int = 5) -> None:
    """Process a row of the input table""" # TODO: make these docstrings actually useful
    # Get RT data
    # Get the schedule gtfs dataset key
    gtfs_dataset_key = (
        gtfs_utils_v2.schedule_daily_feed_to_gtfs_dataset_name(
            selected_date=rt_date, keep_cols=["name", "gtfs_dataset_key"]
        )
        .set_index("name")
        .at[schedule_name, "gtfs_dataset_key"]
    )
    
    print("key", gtfs_dataset_key)
    # Get the merged schedule/stop times table
    schedule_rt_stop_times_single_agency = _filter_non_rt_trips(
        get_schedule_rt_stop_times_table(gtfs_dataset_key, rt_date),
        col.DEFAULT_COLUMN_MAP,
    ).reset_index(drop=True)

    # Impute certain unrealistic (first/last, nonmonotonic, short gap) stop times
    # Logic here is wip
    schedule_rt_stop_times_single_agency["gap_imputed_sec"] = impute_unrealistic_rt_times(
        schedule_rt_stop_times_single_agency,
        max_gap_length=max_stop_gap,
        columns=col.DEFAULT_COLUMN_MAP,
    )

    # Load the schedule feed using gtfs-lite and filter it
    feed = GTFS.load_zip(schedule_local_path)
    feed_filtered = subset_schedule_feed_to_one_date(
        feed, dt.date.fromisoformat(rt_date)
    )

    # Generate the feed based on the imputed rt times and the downloaded schedule feed
    output_feed = make_retrospective_feed_single_date(
        filtered_input_feed=feed_filtered,
        stop_times_table=schedule_rt_stop_times_single_agency,
        stop_times_desired_columns=[
            "trip_id",
            "arrival_time",
            "departure_time" "drop_off_type",
            "pickup_type",
            "stop_headsign",
            "stop_id",
            "stop_sequence",
        ],
        stop_times_table_columns={
            **col.DEFAULT_COLUMN_MAP,
            col.RT_ARRIVAL_SEC: "gap_imputed_sec",
        },
    )

    print(f"Saving feed to {output_local_name}")
    # Save the output to a zip file
    output_feed.write_zip(output_local_name)
    return output_local_name

if __name__ == "__main__":
    # Read command line args
    parser = argparse.ArgumentParser()
    parser.add_argument("input_table", type=str) # The table with input information
    args = parser.parse_args()
    # Read the input table
    input_table = pd.read_csv(args.input_table)

    # Run process_table_row on the input table
    pd.Series(zip(
      input_table["date"], input_table["schedule_local_path"], input_table["schedule_name"], input_table["output_feed_path"]
    )).map(
        lambda row: process_table_row(row[0], row[1], row[2], row[3])
    )
    print("Done")
