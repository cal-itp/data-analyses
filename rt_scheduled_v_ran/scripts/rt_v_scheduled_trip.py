import dask
import dask.dataframe as dd
import geopandas as gpd
import pandas as pd
from calitp_data_analysis.geography_utils import WGS84
import vp_spatial_accuracy
import update_vars2
from segment_speed_utils.project_vars import (
    GCS_FILE_PATH,
    PROJECT_CRS,
    SEGMENT_GCS,
    analysis_date,
)
from segment_speed_utils import helpers, wrangle_shapes

# Times
import datetime
import sys
from loguru import logger

# first install rt_segment_speeds, then _shared_utils
# cd rt_segment_speeds && pip install -r requirements.txt && cd data-analyses/_shared_utils && make setup_env
# cd ../data-analyses/rt_scheduled_v_ran/scripts 

# LOAD FILES
def load_trip_speeds(analysis_date):
    df = pd.read_parquet(
    f"{SEGMENT_GCS}trip_summary/trip_speeds_{analysis_date}.parquet",
    columns=[
        "trip_instance_key",
        "speed_mph",
        "route_id",
        "time_of_day",
        "service_minutes",
    ])
    
    return df

def load_vp_usable(analysis_date):

    # Delete schedule_gtfs_dataset_key later
    df = dd.read_parquet(
        f"{SEGMENT_GCS}vp_usable_{analysis_date}",
        columns=[
            "schedule_gtfs_dataset_key",
            "trip_instance_key",
            "location_timestamp_local",
            "x",
            "y",
            "vp_idx",
        ],
    )

    # Create a copy of location timestamp for the total_trip_time function
    # to avoid type setting warning
    df["max_time"] = df.location_timestamp_local
    return df

# UPDATE COMPLETENESS
def total_trip_time(vp_usable_df: pd.DataFrame):
    """
    For each trip: find the total service minutes
    recorded in real time data so we can compare it with
    scheduled service minutes.
    """
    subset = ["location_timestamp_local", "trip_instance_key", "max_time"]
    vp_usable_df = vp_usable_df[subset]

    # Find the max and the min time based on location timestamp
    df = (
        vp_usable_df.groupby(["trip_instance_key"])
        .agg({"location_timestamp_local": "min", "max_time": "max"})
        .reset_index()
        .rename(columns={"location_timestamp_local": "min_time"})
    )

    # Find total rt service mins and add an extra minute
    df["rt_service_min"] = (df.max_time - df.min_time) / pd.Timedelta(minutes=1) + 1

    # Return only one row per trip with 2 columns: trip instance key & total trip time
    df = df.drop(columns=["max_time", "min_time"])

    return df

def trips_by_one_min(vp_usable_df: pd.DataFrame):
    """
    For each trip: count how many rows are associated with each minute
    then tag whether or not a minute has 2+ pings. 
    """
    subset = ["location_timestamp_local", "trip_instance_key", "vp_idx"]
    vp_usable_df = vp_usable_df[subset]

    # Find number of pings each minute
    df = (
        vp_usable_df.groupby(
            [
                "trip_instance_key",
                pd.Grouper(key="location_timestamp_local", freq="1Min"),
            ]
        )
        .vp_idx.count()
        .reset_index()
        .rename(columns={"vp_idx": "number_of_pings_per_minute"})
    )

    # Determine which rows have 2+ pings per minute
    df = df.assign(
        min_w_atleast2_trip_updates=df.apply(
            lambda x: 1 if x.number_of_pings_per_minute >= 2 else 0, axis=1
        )
    )
    
    df = df.drop(columns = ['location_timestamp_local'])
    return df

def update_completeness(df: pd.DataFrame):
    """
    For each trip: find the median GTFS pings per minute,
    the total minutes with at least 1 GTFS ping per minute,
    and total minutes with at least 2 GTFS pings per minute.
    
    Use result  from trips_by_one_min as the argument.
    """
    # Need a copy of numer of pings per minute to count for total minutes w gtfs
    df["total_min_w_gtfs"] = df.number_of_pings_per_minute
    
    # Find the total min with at least 2 pings per min
    df = (
        df.groupby(["trip_instance_key"])
        .agg(
            {
                "min_w_atleast2_trip_updates": "sum",
                "number_of_pings_per_minute": "mean",
                "total_min_w_gtfs": "count",
            }
        )
        .reset_index()
        .rename(
            columns={
                "number_of_pings_per_minute": "avg_pings_per_min",
            }
        )
    )

    return df

# SPATIAL ACCURACY
def grab_shape_keys_in_vp(vp_usable: dd.DataFrame, analysis_date: str) -> pd.DataFrame:
    """
    Subset raw vp and find unique trip_instance_keys.
    Create crosswalk to link trip_instance_key to shape_array_key
    to find trips for the analysis date that have shapes.
    """
    vp_usable = (
        vp_usable[["trip_instance_key"]]
        .drop_duplicates()
        .reset_index(drop=True)
    )

    trips_with_shape = helpers.import_scheduled_trips(
        analysis_date,
        columns=["trip_instance_key", "shape_array_key"],
        get_pandas=True,
    )

    # Only one row per trip/shape: trip_instance_key and shape_array_key are the only cols left
    m1 = dd.merge(vp_usable, trips_with_shape, on="trip_instance_key", how="inner")

    return m1

def buffer_shapes(
    trips_with_shape: pd.DataFrame,
    analysis_date: str,
    buffer_meters: int = 35,
):
    """
    Filter scheduled shapes down to the shapes that appear in vp.
    Buffer these.

    Attach the shape geometry for a subset of shapes or trips.
    """
    subset = trips_with_shape.shape_array_key.unique().compute().tolist()

    shapes = helpers.import_scheduled_shapes(
        analysis_date,
        columns=["shape_array_key", "geometry"],
        filters=[[("shape_array_key", "in", subset)]],
        crs=PROJECT_CRS,
        get_pandas=False,
    ).pipe(helpers.remove_shapes_outside_ca)

    # to_crs takes awhile, so do a filtering on only shapes we need
    shapes = shapes.assign(geometry=shapes.geometry.buffer(buffer_meters))

    trips_with_shape_geom = dd.merge(
        shapes, trips_with_shape, on="shape_array_key", how="inner"
    )

    trips_with_shape_geom = trips_with_shape_geom.compute()
    return trips_with_shape_geom

def vp_in_shape(
    vp_usable: dd.DataFrame, trips_with_buffered_shape: gpd.GeoDataFrame
) -> gpd.GeoDataFrame:

    keep = ["trip_instance_key", "x", "y", "location_timestamp_local"]
    vp_usable = vp_usable[keep]

    vp_gdf = wrangle_shapes.vp_as_gdf(vp_usable)

    gdf = pd.merge(
        vp_gdf, trips_with_buffered_shape, on="trip_instance_key", how="inner"
    )

    gdf = gdf.assign(is_within=gdf.geometry_x.within(gdf.geometry_y))
    
    to_keep = ["trip_instance_key", "location_timestamp_local", "is_within"]
    gdf = gdf[to_keep]

    return gdf

def total_vp_counts_by_trip(vp: gpd.GeoDataFrame, new_col_title: str):
    """
    Get a count of vp for each trip, whether or not those fall
    within buffered shape or not
    """
    count_vp = (
        vp.groupby("trip_instance_key", observed=True, group_keys=False)
        .agg({"location_timestamp_local": "count"})
        .reset_index()
        .rename(columns={"location_timestamp_local": new_col_title})
    )

    return count_vp

def total_counts(result: dd.DataFrame):

    # Find the total number of vps for each route
    total_vp_df = total_vp_counts_by_trip(result, "total_vp")

    # Find the total number of vps that actually fall within the  route shape
    subset = ["trip_instance_key", "location_timestamp_local"]
    result2 = result.loc[result.is_within == True].reset_index(drop=True)[subset]

    vps_in_shape = total_vp_counts_by_trip(result2, "vp_in_shape")

    # Count total vps for the trip
    count_df = pd.merge(total_vp_df, vps_in_shape, on="trip_instance_key", how="left")

    count_df = count_df.assign(
        vp_in_shape=count_df.vp_in_shape.fillna(0).astype("int32"),
        total_vp=count_df.total_vp.fillna(0).astype("int32"),
    )

    return count_df

# Complete
def vp_usable_metrics(analysis_date:str) -> pd.DataFrame:
    start = datetime.datetime.now()
    print(f"For {analysis_date}, started running script at {start}")
    
    """
    Keep for testing temporarily
    operator = "Bay Area 511 Muni VehiclePositions"
    """
    gtfs_key = "7cc0cb1871dfd558f11a2885c145d144"
    vp_usable = load_vp_usable(analysis_date)
    
    vp_usable = (vp_usable.loc
    [vp_usable.schedule_gtfs_dataset_key ==gtfs_key]
    .reset_index(drop=True))
    
    ## Find total rt service minutes ##
    rt_service_df = total_trip_time(vp_usable)

    time1 = datetime.datetime.now()
    logger.info(f"Rt service min: {time1-start}")
    
    ## Update Completeness ##
    # Find median pings per min, total min with
    # GTFS and total min with at least 2 pings per min.
    trips_by_one_min_df = vp_usable.map_partitions(
    trips_by_one_min,
    meta={
        "trip_instance_key": "object",
        "number_of_pings_per_minute": "int64",
        "min_w_atleast2_trip_updates": "int64",
    },
    align_dataframes=False
    ).persist()
    time2 = datetime.datetime.now()
    logger.info(f"Grouping by each minute: {time2-time1}")
    
    # Final function
    pings_trip_time_df = update_completeness(trips_by_one_min_df)
    time3 = datetime.datetime.now()
    logger.info(f"Spatial accuracy metric: {time3-time2}")
    
    ## Spatial accuracy  ##
    # Determine which trips have shapes associated with them
    trips_with_shapes_df = grab_shape_keys_in_vp(vp_usable, analysis_date)

    # Buffer the shapes 
    buffered_shapes_df = buffer_shapes(trips_with_shapes_df, analysis_date, 35)
    time4 = datetime.datetime.now()
    logger.info(f"Buffering: {time4-time3}")
    
    # Find the vps that fall into buffered shapes
    in_shape_df = vp_usable.map_partitions(
    vp_in_shape,
    buffered_shapes_df,
    meta={
        "trip_instance_key": "object",
        "location_timestamp_local": "datetime64[ns]",
        "is_within":"bool",
    },align_dataframes=False).persist()
    time5 = datetime.datetime.now()
    logger.info(f"Find vps that fall into shapes: {time5-time4}")
    
    # Compare total vps for a trip versus total vps that 
    # fell in the recorded shape
    spatial_accuracy_df = in_shape_df.map_partitions(
        total_counts,
        meta={
            "trip_instance_key": "object", 
            "total_vp": "int32", 
            "vp_in_shape": "int32"},
    align_dataframes=False).persist()
    time6 = datetime.datetime.now()
    logger.info(f"Spatial accuracy grouping metric: {time6-time5}")
    
    # Load trip speeds
    trip_speeds_df = load_trip_speeds(analysis_date)
    
    # Merges
    rt_service_df = rt_service_df.compute()
    pings_trip_time_df = pings_trip_time_df.compute()
    spatial_accuracy_df = spatial_accuracy_df.compute()
    
    m1 = (rt_service_df.merge(pings_trip_time_df, on = "trip_instance_key", how = "outer")
         .merge(spatial_accuracy_df, on ="trip_instance_key", how = "outer")
         .merge(trip_speeds_df, on ="trip_instance_key", how = "outer"))
    
    m1.to_parquet(f"{GCS_FILE_PATH}rt_vs_schedule/trip_{analysis_date}_metrics.parquet")
    time7 = datetime.datetime.now()
    logger.info(f"Total run time for metrics on {analysis_date}: {time7-start}")

if __name__ == "__main__":
    for date in update_vars2.analysis_date_list:
        LOG_FILE = "../logs/rt_v_scheduled_trip_metrics.log"
        logger.add(LOG_FILE, retention="3 months")
        logger.add(sys.stderr, 
                   format="{time:YYYY-MM-DD at HH:mm:ss} | {level} | {message}", 
                   level="INFO")

        vp_usable_metrics(date)
        print('Done')