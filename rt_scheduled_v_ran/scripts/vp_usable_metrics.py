import datetime

import dask
import dask.dataframe as dd
import geopandas as gpd
import pandas as pd
# from calitp_data_analysis import utils
from calitp_data_analysis.geography_utils import WGS84
import vp_spatial_accuracy
from segment_speed_utils import helpers
from segment_speed_utils.project_vars import (
    GCS_FILE_PATH,
    PROJECT_CRS,
    SEGMENT_GCS,
    analysis_date,
)

# Times
import sys
from loguru import logger

# cd rt_segment_speeds && pip install -r requirements.txt && cd

# UPDATE COMPLETENESS
def pings_trip_time(vp_usable_df: pd.DataFrame):

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
        minutes_w_atleast2_trip_updates=df.apply(
            lambda x: 1 if x.number_of_pings_per_minute >= 2 else 0, axis=1
        )
    )

    # Need a copy of loc-timestamp-local to get max time
    df["max_time"] = df.location_timestamp_local
    
    # Need a copy of numer of pings per minute to count
    # for total minutes w gtfs
    df["total_minute_w_gtfs"] = df.number_of_pings_per_minute

    # Find the min time for each trip and sum up total min with at least 2 pings per min
    df = (
        df.groupby(["trip_instance_key"])
        .agg(
            {
                "location_timestamp_local": "min",
                "max_time": "max",
                "minutes_w_atleast2_trip_updates": "sum",
                "number_of_pings_per_minute": "median",
                "total_minute_w_gtfs": "count",
            }
        )
        .reset_index()
        .rename(
            columns={
                "location_timestamp_local": "min_time",
                "number_of_pings_per_minute": "median_pings_per_min",
            }
        )
    )

    # Find total trip time and add an extra minute
    df["total_trip_time"] = (df.max_time - df.min_time) / pd.Timedelta(minutes=1) + 1

    df = df.drop(columns=["min_time", "max_time"])
    return df

def grab_shape_keys_in_vp(vp_usable: dd.DataFrame, analysis_date: str):
    """
    Subset raw vp and find unique trip_instance_keys.
    Create crosswalk to link trip_instance_key to shape_array_key.
    """
    vp_usable = (
        vp_usable[["trip_instance_key"]].drop_duplicates().reset_index(drop=True)
    )

    trips_with_shape = helpers.import_scheduled_trips(
        analysis_date,
        columns=["trip_instance_key", "shape_array_key"],
        get_pandas=True,
    )

    # Only one row per trip/shape
    # trip_instance_key and shape_array_key are the only 2 cols left
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

# SPATIAL ACCURACY
def vp_in_shape(
    vp_usable: dd.DataFrame, trips_with_buffered_shape: gpd.GeoDataFrame
) -> gpd.GeoDataFrame:

    keep = ["trip_instance_key", "x", "y", "location_timestamp_local"]
    vp_usable = vp_usable[keep]

    vp_gdf = gpd.GeoDataFrame(
        vp_usable, geometry=gpd.points_from_xy(vp_usable.x, vp_usable.y), crs=WGS84
    ).to_crs(PROJECT_CRS)

    gdf = pd.merge(
        vp_gdf, trips_with_buffered_shape, on="trip_instance_key", how="inner"
    )
    
    gdf = gdf.assign(is_within=gdf.geometry_x.within(gdf.geometry_y))
    gdf = gdf[["trip_instance_key", "location_timestamp_local", "is_within"]]
    
    return gdf

def total_counts(result: dd.DataFrame):
    
    total_vp = vp_spatial_accuracy.total_vp_counts_by_trip(result)
    
    result2 = result.loc[result.is_within == True].reset_index(drop = True)
    result2 = result2[["trip_instance_key", "location_timestamp_local"]]    
    vps_in_shape = (
        result2.groupby("trip_instance_key", observed=True, group_keys=False)
        .agg({"location_timestamp_local": "count"})
        .reset_index()
        .rename(columns={"location_timestamp_local": "vp_in_shape"})
    )

    # Count total vps for the trip
    # total vp by trip can be done on vp_usable / break apart from vp_in_shape

    count_df = pd.merge(total_vp, vps_in_shape, on="trip_instance_key", how="left")

    count_df = count_df.assign(
        vp_in_shape=count_df.vp_in_shape.fillna(0).astype("int32"),
        total_vp=count_df.total_vp.fillna(0).astype("int32"),
    )
    
    return count_df

# SPEEDS
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

# Complete
def vp_usable_metrics(analysis_date:str) -> pd.DataFrame:
    
    """
    Keep for testing
    operator = "Bay Area 511 Muni VehiclePositions"
    gtfs_key = "7cc0cb1871dfd558f11a2885c145d144"

    vp_usable= dd.read_parquet(
    f"{SEGMENT_GCS}vp_usable_{analysis_date}",
    filters=[
        [
            ("gtfs_dataset_name", "==", operator),
            ("schedule_gtfs_dataset_key", "==", gtfs_key),
        ]
    ],
    ) 
    """
    vp_usable = dd.read_parquet(f"{SEGMENT_GCS}vp_usable_{analysis_date}")
    
    ## Update Completeness ##
    
    # Find total min with gtfs, total trip time, 
    # median pings per minute 
    pings_trip_time_df = vp_usable.map_partitions(
    pings_trip_time,
    meta={
        "trip_instance_key": "object",
        "minutes_w_atleast2_trip_updates": "int64",
        "median_pings_per_min": "int64",
        "total_minute_w_gtfs": "int64",
        "total_trip_time": "float64",
    },
    align_dataframes=False).persist()

    ## Spatial accuracy  ##
    
    # Determine which trips have shapes associated with them
    trips_with_shapes_df = grab_shape_keys_in_vp(vp_usable, analysis_date)
    
    # Buffer the shapes 
    buffered_shapes_df = buffer_shapes(trips_with_shapes_df, analysis_date, 35)
    
    # Find the vps that fall into buffered shapes
    in_shape_df = vp_usable.map_partitions(
    vp_in_shape,
    buffered_shapes_df,
    meta={
        "trip_instance_key": "object",
        "location_timestamp_local": "datetime64[ns]",
        "is_within":"bool",
    },
    align_dataframes=False).persist()
    
    # Compare total vps for a trip versus total vps that 
    # fell in the recorded shape
    spatial_accuracy_df = in_shape_df.map_partitions(
    total_counts,
    meta={"trip_instance_key": "object", "total_vp": "int32", "vp_in_shape": "int32"},
    align_dataframes=False,).persist()
    
    # Load trip speeds
    trip_speeds_df = load_trip_speeds(analysis_date)
    
    # Merges
    pings_trip_time_df = pings_trip_time_df.compute()
    spatial_accuracy_df = spatial_accuracy_df.compute()
    
    m1 = (
    pings_trip_time_df.merge(spatial_accuracy_df, on=["trip_instance_key"], how="outer")
    .merge(trip_speeds_df, on=["trip_instance_key"], how="outer")
    )
    
    m1.to_parquet('./vp_usable_metrics.parquet')

if __name__ == "__main__":
    start = datetime.datetime.now()
    LOG_FILE = "../logs/vp_usable_test.log"
    logger.add(LOG_FILE, retention="3 months")
    logger.add(sys.stderr, 
               format="{time:YYYY-MM-DD at HH:mm:ss} | {level} | {message}", 
               level="INFO")
    end = datetime.datetime.now()
    logger.info(f"Analysis date: {analysis_date}, started at {start}")
    vp_usable_metrics(analysis_date)
    logger.info(f"Ended at {end}, took {end - start} min to run")
    print('done')