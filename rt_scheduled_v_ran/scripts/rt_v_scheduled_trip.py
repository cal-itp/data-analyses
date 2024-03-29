"""
Generate RT vs schedule metrics for trip-level.
"""
import dask.dataframe as dd
import datetime
import geopandas as gpd
import pandas as pd
import sys

from dask import delayed, compute
from loguru import logger

from calitp_data_analysis.geography_utils import WGS84
from update_vars import CONFIG_DICT, analysis_date_list

from segment_speed_utils.project_vars import (
    PROJECT_CRS,
    SEGMENT_GCS,
    RT_SCHED_GCS,
)
from segment_speed_utils import (gtfs_schedule_wrangling, helpers, 
                                 metrics,
                                 segment_calcs)


# UPDATE COMPLETENESS
def vp_trip_time(vp: pd.DataFrame,) -> pd.DataFrame:
    """
    For each trip: find the total service minutes
    recorded in real time data so we can compare it with
    scheduled service minutes.
    """
    vp = segment_calcs.convert_timestamp_to_seconds(
        vp, ["location_timestamp_local"])
    
    # Find the max and the min time based on location timestamp
    grouped_df = vp.groupby("trip_instance_key", 
                            observed=True, group_keys=False)
    start_time = (
        grouped_df
        .agg({
            "location_timestamp_local": "count",
            "location_timestamp_local_sec": "min"
        })
        .reset_index()
        .rename(columns={
            "location_timestamp_local": "total_vp",
            "location_timestamp_local_sec": "min_time"
        })
    )
    
    end_time = (
        grouped_df
        .agg({"location_timestamp_local_sec": "max"})
        .reset_index()
        .rename(columns={"location_timestamp_local_sec": "max_time"})
    )
    
    df = start_time.merge(
        end_time,
        on = "trip_instance_key",
        how = "inner"
    )
    
    # Find total rt service minutes (take difference in seconds)
    df = df.assign(
        rt_service_minutes = (df.max_time - df.min_time) / 60
    ).drop(columns = ["max_time", "min_time"])

    return df


def vp_one_minute_interval_metrics(
    vp: pd.DataFrame,
) -> pd.DataFrame:
    """
    For each trip: count how many rows are associated with each minute
    then tag whether or not a minute has 2+ pings. 
    """
    time_col = "location_timestamp_local"
    grouped_df = vp.groupby(
        ["trip_instance_key", 
         pd.Grouper(key = time_col, freq="1Min")
        ]
    )
    
    # Find number of pings each minute
    df = (
        grouped_df
        .vp_idx
        .count()
        .reset_index()
        .rename(columns={"vp_idx": "n_pings_per_min"})
    )

    # Determine which rows have 2+ pings per minute
    df = df.assign(
        at_least2=df.apply(
            lambda x: 1 if x.n_pings_per_min >= 2 else 0, axis=1
        )
    )
    
    df2 = (df.groupby("trip_instance_key", observed=True, group_keys=False)
           .agg({
               # this is time frequency grouper
               time_col: "count",
               "at_least2": "sum"
           })
           .rename(columns = {
               time_col: "minutes_atleast1_vp", 
               "at_least2": "minutes_atleast2_vp"
           }).reset_index()
          )
    
    return df2


def basic_counts_by_vp_trip(analysis_date: str) -> pd.DataFrame:
    """
    Calculate vp trip metrics that are strictly tabular
    that can be easily generated.
    Use dask delayed because pd.Grouper is pandas only.
    """
    trip_cols = ["trip_instance_key"]
    
    vp = delayed(pd.read_parquet)(
        f"{SEGMENT_GCS}vp_usable_{analysis_date}",
        columns = trip_cols + ["location_timestamp_local", "vp_idx"],
    )
    
    rt_service_df = vp_trip_time(vp)
    rt_time_coverage = vp_one_minute_interval_metrics(vp)
   
    results = delayed(pd.merge)(
        rt_service_df,
        rt_time_coverage,
        on = trip_cols,
        how = "inner"
    )
    
    results = compute(results)[0]
    results.to_parquet(
        f"{RT_SCHED_GCS}vp_trip/intermediate/"
        f"trip_stats_{analysis_date}.parquet")
    
    return
 
    
# SPATIAL ACCURACY 
def buffer_shapes(
    analysis_date: str,
    buffer_meters: int = 35,
    **kwargs
) -> gpd.GeoDataFrame:
    """
    Buffer shapes for shapes that are present in vp.
    """ 
    # Remove certain Amtrak routes
    amtrak_outside_ca = gtfs_schedule_wrangling.amtrak_trips(
        analysis_date, inside_ca = False).shape_array_key.unique().tolist()
    
    shapes = helpers.import_scheduled_shapes(
        analysis_date,
        columns=["shape_array_key", "geometry"],
        crs=PROJECT_CRS,
        get_pandas=True,
        **kwargs
    ).dropna(
        subset="geometry"
    ).query("shape_array_key not in @amtrak_outside_ca")
    
    shapes = shapes.assign(
        geometry = shapes.geometry.buffer(buffer_meters)
    )
    
    return shapes


def count_vp_in_shape(
    vp: gpd.GeoDataFrame,
    shapes: gpd.GeoDataFrame
) -> gpd.GeoDataFrame:
    """
    For each trip, count the number of vp within a 35meter buffer of
    scheduled shape.
    """  
    gdf = pd.merge(
        shapes,
        vp,
        on = "shape_array_key",
        how = "inner"
    )
    
    # Ask if vp point is within buffered shape polygon and 
    # only keep it if it is     
    vp_point = gpd.points_from_xy(
        gdf.x, gdf.y, crs = WGS84
    ).to_crs(PROJECT_CRS)
           
    is_within = vp_point.within(gdf.geometry)
    
    gdf = gdf.assign(
        vp_in_shape=is_within
    )[["trip_instance_key", "vp_in_shape"]].query('vp_in_shape==True')
    
    gdf2 = (gdf
            .groupby("trip_instance_key", 
                     observed=True, group_keys=False)
            .agg({"vp_in_shape": "sum"})
            .reset_index()
            .astype({"vp_in_shape": "int32"})
           )
    
    return gdf2


def spatial_accuracy_count(analysis_date: str):
    """
    Merge vp with shape_array_key and shape_geometry
    and count how many vp per trip fall within shape.
    """
    buffer_meters = 35
    
    trip_to_shape = helpers.import_scheduled_trips(
        analysis_date,
        columns = ["trip_instance_key", "shape_array_key"],
        get_pandas = True
    )

    vp_usable = dd.read_parquet(
        f"{SEGMENT_GCS}vp_usable_{analysis_date}",
        columns=["trip_instance_key", "x", "y"],
    ).merge(
        trip_to_shape,
        on = "trip_instance_key",
        how = "inner"
    ).repartition(npartitions=150).persist()
        
    shapes_in_vp = vp_usable.shape_array_key.unique().compute().tolist()
    
    shapes = buffer_shapes(
        analysis_date, 
        buffer_meters = buffer_meters,
        filters = [[("shape_array_key", "in", shapes_in_vp)]], 
    )        
          
    results = vp_usable.map_partitions(
        count_vp_in_shape,
        shapes,
        meta = {
            "trip_instance_key": "str",
            "vp_in_shape": "int32"},
        align_dataframes = False
    )
        
    results = results.compute()
            
    results.to_parquet(
        f"{RT_SCHED_GCS}vp_trip/intermediate/"
        f"spatial_accuracy_{analysis_date}.parquet",
    )
    
    return


# Complete
def rt_schedule_trip_metrics(
    analysis_date: str, 
    dict_inputs: dict
) -> pd.DataFrame:
    """

    """    
    start = datetime.datetime.now()

    # number of minutes to determine trip lateness    
    early = dict_inputs["early_trip_minutes"]
    late = dict_inputs["late_trip_minutes"]
    TRIP_EXPORT = dict_inputs["trip_metrics"]
    '''
    basic_counts_by_vp_trip(analysis_date)
    
    time1 = datetime.datetime.now()
    logger.info(f"tabular trip metrics {analysis_date}: {time1 - start}")
    
    spatial_accuracy_count(analysis_date)
    
    time2 = datetime.datetime.now()
    logger.info(f"spatial trip metrics {analysis_date}: {time2 - time1}")
    '''
    ## Merges ##
    rt_service_df = pd.read_parquet(
        f"{RT_SCHED_GCS}vp_trip/intermediate/"
        f"trip_stats_{analysis_date}.parquet"
    )
    spatial_accuracy_df = pd.read_parquet(
        f"{RT_SCHED_GCS}vp_trip/intermediate/"
        f"spatial_accuracy_{analysis_date}.parquet"
    )
    
    # Merge and add metrics and clean up columns
    df = pd.merge(
        rt_service_df,
        spatial_accuracy_df, 
        on = "trip_instance_key", 
        how = "outer"
    )
    
    # Add route id, direction id, off_peak, and time_of_day columns. 
    route_info = gtfs_schedule_wrangling.attach_scheduled_route_info(
        analysis_date
    )
    
    df2 = pd.merge(
        df,
        route_info,
        on = "trip_instance_key",
        how = "inner"
    ).pipe(
        gtfs_schedule_wrangling.add_peak_offpeak_column
    ).pipe(
        metrics.derive_rt_vs_schedule_metrics
    ).pipe(
        metrics.derive_trip_comparison_metrics, early, late
    )
    
    order_first = ["schedule_gtfs_dataset_key", 
                   "trip_instance_key", "route_id", "direction_id",
                   "scheduled_service_minutes"]
    other_cols = [c for c in df2.columns if c not in order_first]
    df2 = df2.reindex(columns = order_first + other_cols)
    
    # Save
    df2.to_parquet(f"{RT_SCHED_GCS}{TRIP_EXPORT}_{analysis_date}.parquet")

    time3 = datetime.datetime.now()
    logger.info(f"Total run time for metrics on {analysis_date}: {time3 - start}")
    
    
if __name__ == "__main__":
    
    LOG_FILE = "../logs/rt_v_scheduled_trip_metrics.log"
    logger.add(LOG_FILE, retention="3 months")
    logger.add(sys.stderr, 
               format="{time:YYYY-MM-DD at HH:mm:ss} | {level} | {message}", 
               level="INFO")
    
    from shared_utils import rt_dates
    oct_week = rt_dates.get_week("oct2023", exclude_wed=True)
    apr_week = rt_dates.get_week("apr2023", exclude_wed=True)

    analysis_date_list = rt_dates.y2024_dates + rt_dates.y2023_dates + oct_week + apr_week
    
    for date in analysis_date_list:
        rt_schedule_trip_metrics(date, CONFIG_DICT)
