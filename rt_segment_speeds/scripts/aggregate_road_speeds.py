"""
Aggregate speeds to road segments
"""
import dask.dataframe as dd
import datetime
import pandas as pd
import geopandas as gpd
import sys

from calitp_data_analysis import utils
from calitp_data_analysis.geography_utils import WGS84
from loguru import logger

from segment_speed_utils import gtfs_schedule_wrangling, helpers
from segment_speed_utils.project_vars import SEGMENT_GCS, GTFS_DATA_DICT, PROJECT_CRS, SHARED_GCS


def explode_road_segment_speeds(
    analysis_date: str,
    segment_type: str = "modeled_road_segments"
) -> pd.DataFrame:
    """
    Turn results stored as arrays into long df.
    Add extra filtering for vp that doesn't travel enough of road segment.
    """
    SPEEDS_WIDE = GTFS_DATA_DICT[segment_type].speeds_wide

    # theoretically at least 2 positions that are captured from GTFS RT
    MIN_SECONDS = GTFS_DATA_DICT[segment_type].min_seconds_elapsed
    # if these are 1 km segments, covering at least 25% of the segment?
    MIN_METERS = GTFS_DATA_DICT[segment_type].min_meters_elapsed 
        
    df = dd.read_parquet(
        f"{SEGMENT_GCS}{SPEEDS_WIDE}_{analysis_date}.parquet",
    ).repartition(npartitions=20)
    
    # are we also removing last obs?
    df = df.assign(
        segment_sequence = df.apply(
            lambda x: x.segment_sequence[:-1], 
            axis=1, meta = ("segment_sequence", "str")
    ))

    df_exploded = df.explode(["segment_sequence", 
         "meters_elapsed", "seconds_elapsed", "speed_mph"]
    ).dropna(
        subset=["speed_mph"]
    ).astype({
        "meters_elapsed": "float",
        "seconds_elapsed": "int",
        "speed_mph": "float"
    })

    # TODO: For roads, especially near intersections, 
    # we might be grabbing very short portions of vp_path
    df_exploded2 = df_exploded[
        (df_exploded.meters_elapsed >= MIN_METERS) & 
        (df_exploded.seconds_elapsed >= MIN_SECONDS)
    ].reset_index(drop=True).persist()
        
    return df_exploded2


def attach_natural_identifiers(
    df: pd.DataFrame
) -> pd.DataFrame:
    """
    Attach necessary GTFS identifiers for 
    aggregated speeds across operators and routes.
    """
    trips = helpers.import_scheduled_trips(
        analysis_date,
        columns = ["trip_instance_key", "name", 
                   "route_id"],
        get_pandas = True
    )
    
    time_of_day = gtfs_schedule_wrangling.get_trip_time_buckets(analysis_date)
    
    df2 = dd.merge(
        df,
        time_of_day,
        on = "trip_instance_key",
        how = "inner"
    ).merge(
        trips,
        on = "trip_instance_key",
        how = "inner"
    )

    return df2


def aggregate_speeds(
    df: pd.DataFrame,
    group_cols: list = ["linearid", "segment_sequence", "time_of_day"]
) -> pd.DataFrame:
    """
    Aggregate by road segments + time_of_day.
    """
    # Get list of operator names
    operator_list = (
        df[group_cols + ["name"]]
        .drop_duplicates()
        .groupby(group_cols, group_keys=False)
        .agg({"name": lambda x: sorted(list(x))})
        .reset_index()
    ).rename(columns = {"name": "operators"})

    # Basic counts of n_trips, n_routes, n_operators
    df2 = (
        df
       .groupby(group_cols, group_keys=False)
       .agg({
            "speed_mph": "mean",
            "trip_instance_key": "nunique",
            "name": "nunique",
            "route_id": "nunique"
        }).reset_index()
        .rename(columns = {
            "trip_instance_key": "n_vp_trips",
            "name": "n_operators",
            "route_id": "n_routes"
       })
    ).merge(
        operator_list,
        on = group_cols,
        how = "inner"
    )
    
    return df2


def merge_in_road_segment_geom(
    df: pd.DataFrame
) -> gpd.GeoDataFrame:
    """
    Merge in 1 km road segments.
    """
    road_segments = gpd.read_parquet(
        f"{SHARED_GCS}segmented_roads_onekm_2020.parquet",
        filters = [[("linearid", "in", df.linearid)]],
        columns = ["linearid", "mtfcc", "fullname", "geometry", "segment_sequence"]
    ).to_crs(WGS84)

    gdf = pd.merge(
        road_segments,
        df,
        on = ["linearid", "segment_sequence"],
        how = "inner"
    )
    
    return gdf


if __name__ == "__main__":
    
    LOG_FILE = "../logs/modeled_road_speeds.log"
    
    logger.add(LOG_FILE, retention="3 months")
    logger.add(sys.stderr, 
               format="{time:YYYY-MM-DD at HH:mm:ss} | {level} | {message}", 
               level="INFO")   
    
    from segment_speed_utils.project_vars import test_dates
    
    for analysis_date in test_dates:
        
        start = datetime.datetime.now()
        
        segment_type = "modeled_road_segments"
        SPEEDS_FILE = GTFS_DATA_DICT[segment_type].speeds
        AGGREGATED_SPEEDS_FILE = GTFS_DATA_DICT[segment_type].segment_timeofday
        
        df = explode_road_segment_speeds(
            analysis_date, 
            segment_type
        ).pipe(attach_natural_identifiers)
        
        df = df.compute()
            
        df.to_parquet(
            f"{SEGMENT_GCS}{SPEEDS_FILE}_{analysis_date}.parquet"
        )
        
        time1 = datetime.datetime.now()
        logger.info(
            f"{segment_type}  {analysis_date}: speeds wide to long: {time1 - start}"
        )
        
        del df
        
        # Add additional step to aggregate across operators
        # different than existing average_segment_speeds
        gdf = pd.read_parquet(
            f"{SEGMENT_GCS}{SPEEDS_FILE}_{analysis_date}.parquet"
        ).pipe(
            aggregate_speeds
        ).pipe(
            merge_in_road_segment_geom
        )
        
        utils.geoparquet_gcs_export(
            gdf,
            SEGMENT_GCS,
            f"{AGGREGATED_SPEEDS_FILE}_{analysis_date}"
        )
        
        end = datetime.datetime.now()
        logger.info(
            f"{segment_type}  {analysis_date}: aggregated road segment speeds: {end - time1}"
        )        
        
