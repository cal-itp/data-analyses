import argparse
import datetime as dt
import pathlib
from typing import Iterable

import geopandas as gpd
import numpy as np
import pandas as pd
import retrospective_feed_generation.columns as col
from calitp_data_analysis.geography_utils import CA_NAD83Albers_m
from gtfslite import GTFS
from raw_feed_download_utils.download_data import download_feeds
from raw_feed_download_utils.get_feed_info import get_feed_info
from retrospective_feed_generation.gtfs_utils import *
from retrospective_feed_generation.retrospective_feed_generation import *
from retrospective_feed_generation.warehouse_utils import *
from shared_utils import catalog_utils, gtfs_utils_v2, rt_dates
from tqdm import tqdm


def process_all_feeds(
    rt_dates: Iterable[dt.date],
    schedule_local_paths: Iterable[str],
    schedule_names: Iterable[str],
    output_local_paths: Iterable[str],
    max_stop_gap: int = 5,
) -> None:
    """Process a set of feeds given dates, schedule paths, schedule names, and output paths"""
    gtfs_dataset_key_dict = {}
    schedule_rt_stop_times_table_dict = {}
    # For each date, get the warehouse data corresponding with that date and the requested feeds
    for rt_date in np.unique(rt_dates):
        # Get gtfs dataset keys from the warehouse for the target date
        gtfs_dataset_keys = (
            gtfs_utils_v2.schedule_daily_feed_to_gtfs_dataset_name(
                selected_date=rt_date, keep_cols=["name", "gtfs_dataset_key"]
            )
            .set_index("name")
            .loc[np.unique(schedule_names), "gtfs_dataset_key"]
        )
        # Get the schedule rt stop times table for the specified tables, and save them in a dict keyed by date
        schedule_rt_stop_times_table_dict[rt_date] = get_schedule_rt_stop_times_table(gtfs_dataset_keys.values, rt_date)
        # Save the set the series of gtfs dataset keys, and save them in a dict keyed by date
        gtfs_dataset_key_dict[rt_date] = gtfs_dataset_keys
    # Create a dict of gtfs dataset keys keyed by schedule name (index) and date (columns)
    gtfs_dataset_key_df = pd.DataFrame(gtfs_dataset_key_dict)
    # For each feed, get the input table and generate a feed. Record whether there were any rt data downloaded
    success_status = []
    for rt_date, schedule_name, schedule_local_path, output_local_path in zip(
        rt_dates, schedule_names, schedule_local_paths, output_local_paths
    ):
        gtfs_dataset_key = gtfs_dataset_key_df.at[schedule_name, rt_date]
        schedule_rt_table = schedule_rt_stop_times_table_dict[rt_date]
        schedule_rt_table_one_feed = schedule_rt_table.loc[
            schedule_rt_table["schedule_gtfs_dataset_key"] == gtfs_dataset_key
        ]
        rt_times_present = schedule_rt_table_one_feed["rt_arrival_sec"].notna().any()
        success_status.append(rt_times_present)
        # Generate a feed
        if rt_times_present:
            process_individual_feed(
                schedule_rt_table_one_feed,
                rt_date,
                schedule_local_path,
                output_local_path,
                max_stop_gap=max_stop_gap,
            )
    return success_status


def process_individual_feed(
    schedule_rt_table: pd.DataFrame,
    rt_date: dt.date,
    schedule_local_path: str,
    output_local_path: str,
    max_stop_gap: int = 5,
) -> None:
    """Get retrospective accessibility for one row of the input feed"""
    # Get the schedule rt table with trips that did not generate VP trips dropped
    schedule_rt_stop_times_single_agency = filter_non_rt_trips(
        schedule_rt_table,
        col.DEFAULT_COLUMN_MAP,
    ).reset_index(drop=True)

    # Impute certain unrealistic (first/last, nonmonotonic, short gap) stop times
    # Logic here is wip
    schedule_rt_stop_times_single_agency["gap_imputed_sec"] = impute_unrealistic_rt_times(
        schedule_rt_stop_times_single_agency,
        max_gap_length=max_stop_gap,
    )

    # Load the schedule feed using gtfs-lite and filter it
    feed = GTFS.load_zip(schedule_local_path, ignore_optional_files="keep_shapes")
    feed_filtered = subset_schedule_feed_to_one_date(feed, rt_date)

    # Generate the feed based on the imputed rt times and the downloaded schedule feed
    output_feed = make_retrospective_feed_single_date(
        filtered_input_feed=feed_filtered,
        stop_times_table=schedule_rt_stop_times_single_agency,
        stop_times_desired_columns=[
            "trip_id",
            "arrival_time",
            "departure_time",
            "drop_off_type",
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

    print(f"Saving feed to {output_local_path}")
    # Save the output to a zip file
    output_feed.write_zip(output_local_path)
    return output_local_path


DOWNLOADED_FEEDS_FOLDER = "downloaded_schedule_feeds/"
OUTPUT_FOLDER = "./generated_rt_feeds/"

if __name__ == "__main__":

    # Read command line args
    parser = argparse.ArgumentParser()
    parser.add_argument("target_date", type=str)  # The target date, in "2025-11-05"
    parser.add_argument("geometry_path", type=str)  # The table with input information
    args = parser.parse_args()

    # Get feed info
    analysis_geometry = gpd.read_file(args.geometry_path).to_crs(CA_NAD83Albers_m)
    feed_info = get_feed_info(
        target_date=args.target_date,
        lookback_period=dt.timedelta(days=60),
        filter_geometry=analysis_geometry,
        report_unavailable=True,
    ).drop_duplicates()

    # Download feeds
    feed_info["schedule_local_path"] = download_feeds(feed_info, DOWNLOADED_FEEDS_FOLDER)
    feed_info["output_local_path"] = feed_info["schedule_local_path"].map(
        lambda x: str(pathlib.Path(OUTPUT_FOLDER).joinpath(f"rt_{pathlib.Path(x).name}"))
    )

    # Get RT-derived feeds
    feed_info["feed_generated"] = process_all_feeds(
        feed_info["date"],
        feed_info["schedule_local_path"],
        feed_info["gtfs_dataset_name"],
        feed_info["output_local_path"],
    )
    feed_info.to_csv("test_feed_info.csv")

    # Run process_table_row on the input table
    # process_all_feeds(
    #    input_table["date"].values,
    #    input_table["schedule_local_path"].values,
    #    input_table["schedule_name"].values,
    #    input_table["output_local_path"].values,
    # )
    print("Done")
