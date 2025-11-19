"""
Using movingpandas and try to get as many of the
columns we can calculated.
This will make it easier to work into as a Python data model
when we use BigFrames.
"""

import datetime
import sys

import geopandas as gpd
import google.auth
import movingpandas as mpd
import numpy as np
import pandas as pd
import shapely
from loguru import logger
from shared_utils import rt_dates
from update_vars import GTFS_DATA_DICT, SEGMENT_GCS

credentials, project = google.auth.default()


def determine_batches(operator_df: pd.DataFrame, rt_name_col: str = "gtfs_dataset_name") -> dict:
    """
    Use df to pipe this function with the df used.
    https://stackoverflow.com/questions/4843158/how-to-check-if-a-string-is-a-substring-of-items-in-a-list-of-strings
    """

    rt_names = sorted(operator_df[rt_name_col].unique().tolist())

    la_metro_names = [
        "LA Metro Bus",
        "LA Metro Rail",
    ]

    bay_area_large_names = ["AC Transit", "Muni"]

    bay_area_names = ["Bay Area 511"]

    # If any of the large operator name substring is
    # found in our list of names, grab those
    # be flexible bc "Vehicle Positions" and "VehiclePositions" present
    matching1 = [i for i in rt_names if any(name in i for name in la_metro_names)]

    matching2 = [i for i in rt_names if any(name in i for name in bay_area_large_names)]

    remaining_bay_area = [
        i
        for i in rt_names
        if any(name in i for name in bay_area_names) and (i not in matching1) and (i not in matching2)
    ]
    remaining = [i for i in rt_names if (i not in matching1) and (i not in matching2) and (i not in remaining_bay_area)]

    # Batch large operators together and run remaining in 2nd query
    batch_dict = {}

    batch_dict[0] = matching1  # 2 feeds, 14k trips
    batch_dict[1] = remaining_bay_area  # 24 feeds, 13k trips
    batch_dict[2] = matching2  # 2 feeds; 14k trips
    # batch_dict[3] = remaining # 93 feeds, 54-55k trips, this one is huge, split into 3

    n_operators_remaining = len(remaining)
    one_third_operators = n_operators_remaining // 3

    batch_dict[3] = remaining[:one_third_operators]
    batch_dict[4] = remaining[one_third_operators : one_third_operators * 2]
    batch_dict[5] = remaining[one_third_operators * 2 :]

    return batch_dict


def transform_array_of_strings(df: pd.DataFrame) -> pd.DataFrame:
    """
    Try to load an array of points and explode this with
    downloaded df.
    """

    df = df.assign(
        service_date=pd.to_datetime(df.service_date),
        pt_array=df.apply(
            lambda x: np.array(shapely.get_coordinates([shapely.wkt.loads(p) for p in x.pt_array])), axis=1
        ),
    )

    return df


def explode_tuples(gdf: gpd.GeoDataFrame) -> pd.DataFrame:
    """
    Get x, y out so it can be used with movingpandas.TrajectoryCollection.
    Might be able to go back to using fct_vehicle_locations
    if this is a Python data model with BigFrames, since the bottleneck
    is in how many rows we can download
    (1 single day of vp has to be broken into 4+ batches to be put back as gdf)
    movingpandas prefers x, y coordinates, it creates geometry under the hood,
    but didn't find the ability to input geometry column
    """

    gdf = gdf.explode(column=["location_timestamp_pacific", "pt_array"]).reset_index(drop=True)

    gdf = gdf.assign(
        x=gdf.apply(lambda x: x.pt_array[0], axis=1),
        y=gdf.apply(lambda x: x.pt_array[1], axis=1),
    ).drop(columns="pt_array")

    return gdf


def add_movingpandas_deltas(
    gdf: gpd.GeoDataFrame,
    traj_id_col: str = "trip_instance_key",
    obj_id_col: str = None,  # vp_key
    x: str = "x",
    y: str = "y",
    t: str = "location_timestamp_pacific",
) -> pd.DataFrame:
    """
    Take df and make into movingpandas.TrajectoryCollection, then
    add all the columns we can based on our raw vp.
    Decide how to handle it after.

    See if we can grab just the underlying df from this and export.

    We should keep vp_key if we want to be able to keep the vp's location and
    join it back to fct_vehicle_locations.
    """
    start_mp = datetime.datetime.now()
    df = explode_tuples(gdf)

    tc = mpd.TrajectoryCollection(df, traj_id_col=traj_id_col, obj_id_col=obj_id_col, x=x, y=y, t=t)

    # Add all the columns we can add
    tc.add_distance(overwrite=True, name="distance_meters", units="m")
    # tc.add_distance(overwrite=True, name="distance_miles", units="mi")

    t1 = datetime.datetime.now()
    logger.info(f"add distance: {t1 - start_mp}")

    tc.add_timedelta(overwrite=True)

    t2 = datetime.datetime.now()
    logger.info(f"add timedelta: {t2 - t1}")

    tc.add_speed(overwrite=True, name="speed_mph", units=("mi", "h"))
    # tc.add_acceleration(
    #    overwrite=True, name="acceleration_mph_per_sec", units=("mi", "h", "s")
    # )

    t3 = datetime.datetime.now()
    logger.info(f"add speed: {t3 - t2}")

    # tc.add_angular_difference(overwrite=True)
    tc.add_direction(overwrite=True)

    t4 = datetime.datetime.now()
    logger.info(f"add direction: {t4 - t3}")

    result_df = pd.concat(
        [traj.df.drop(columns="geometry").pipe(round_columns) for traj in tc], axis=0, ignore_index=True
    )

    result_arrays = flatten_results(result_df)

    end_mp = datetime.datetime.now()
    logger.info(f"save out df: {end_mp - start_mp}")

    del start_mp, end_mp, t1, t2, t3, t4
    del result_df, tc, df

    return result_arrays


def round_columns(df: pd.DataFrame) -> pd.DataFrame:
    """
    Round to 3 decimal places
    """
    df = df.assign(
        distance_meters=df.distance_meters.round(3),
        speed_mph=df.speed_mph.round(3),
        # acceleration_mph_per_sec = df.acceleration_mph_per_sec.round(3),
        # angular_difference = df.angular_difference.round(3),
        direction=df.direction.round(3),
        timedelta=df.timedelta.dt.total_seconds(),  # timedelta is getting saved out as microseconds
    )

    return df


def flatten_results(results: pd.DataFrame) -> pd.DataFrame:
    """
    Save out an array form of results.
    """
    results_arrays = (
        results.groupby(["service_date", "gtfs_dataset_name", "trip_instance_key"])
        .agg(
            {
                "distance_meters": lambda x: list(x),
                # "distance_miles": lambda x: list(x),
                "timedelta": lambda x: list(x),
                "speed_mph": lambda x: list(x),
                # "acceleration_mph_per_sec": lambda x: list(x),
                # "angular_difference": lambda x: list(x),
                "direction": lambda x: list(x),
            }
        )
        .reset_index()
    )
    return results_arrays


if __name__ == "__main__":

    LOG_FILE = "../logs/movingpandas_pipeline.log"
    logger.add(LOG_FILE, retention="2 months")
    logger.add(sys.stderr, format="{time:YYYY-MM-DD at HH:mm:ss} | {level} | {message}", level="INFO")

    INPUT_FILE = GTFS_DATA_DICT.speeds_tables.vp_path
    EXPORT_FILE = GTFS_DATA_DICT.speeds_tables.vp_trajectory

    analysis_date_list = [rt_dates.DATES[f"{m}2025"] for m in ["sep"]]

    for analysis_date in analysis_date_list:

        start = datetime.datetime.now()

        batch_dict = (
            pd.read_parquet(f"{SEGMENT_GCS}{INPUT_FILE}_{analysis_date}.parquet", columns=["gtfs_dataset_name"])
            .drop_duplicates()
            .pipe(determine_batches, "gtfs_dataset_name")
        )

        for batch_num, batch_list in batch_dict.items():

            start_batch = datetime.datetime.now()
            print(f"start {batch_num}: {batch_list}")

            results = (
                pd.read_parquet(
                    f"{SEGMENT_GCS}{INPUT_FILE}_{analysis_date}.parquet",
                    filters=[[("gtfs_dataset_name", "in", batch_list)]],
                    columns=[
                        "service_date",
                        "gtfs_dataset_name",
                        "gtfs_dataset_key",
                        "trip_instance_key",
                        "location_timestamp_pacific",
                        "pt_array",
                    ],
                )
                .pipe(transform_array_of_strings)
                .pipe(add_movingpandas_deltas)
            )

            results.to_parquet(
                f"{SEGMENT_GCS}" f"{EXPORT_FILE}_flat_{analysis_date}/batch_{batch_num}.parquet", engine="pyarrow"
            )

            del results

            end_batch = datetime.datetime.now()
            logger.info(f"finished {batch_num}: {end_batch - start_batch}")

        end = datetime.datetime.now()
        logger.info(f"moving pandas in batches: {analysis_date}: {end - start}")
