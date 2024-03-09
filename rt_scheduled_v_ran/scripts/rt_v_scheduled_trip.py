import dask.dataframe as dd
import datetime
import geopandas as gpd
import pandas as pd
import sys

from dask import delayed, compute
from loguru import logger

from update_vars import CONFIG_DICT

from segment_speed_utils.project_vars import (
    PROJECT_CRS,
    SEGMENT_GCS,
    RT_SCHED_GCS,
)
from segment_speed_utils import (gtfs_schedule_wrangling, helpers, 
                                 segment_calcs, wrangle_shapes)

# cd rt_segment_speeds && pip install -r requirements.txt && cd ../_shared_utils && make setup_env
# cd ../rt_scheduled_v_ran/scripts && python rt_v_scheduled_trip.py

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
    buffer_meters: int = 35
) -> gpd.GeoDataFrame:
    """
    Buffer shapes for shapes that are present in vp.
    """ 
    shapes = helpers.import_scheduled_shapes(
        analysis_date,
        columns=["shape_array_key", "geometry"],
        crs=PROJECT_CRS,
        get_pandas=True,
    ).dropna(
        subset="geometry"
    )
    
    shapes = shapes.assign(
        geometry = shapes.geometry.buffer(buffer_meters)
    )
    
    return shapes


def count_vp_in_shape(
    gdf: gpd.GeoDataFrame,
) -> gpd.GeoDataFrame:
    """
    For each trip, count the number of vp within a buffered
    scheduled shape.
    """     
    # Ask if vp point is within buffered shape polygon and 
    # only keep it if it is 
    gdf = gdf.rename(
        columns = {"geometry": "shape_geometry"}
    ).set_geometry("shape_geometry")
    
    gdf = wrangle_shapes.vp_as_gdf(gdf, crs = PROJECT_CRS).set_geometry("geometry")
            
    gdf = gdf.assign(
        vp_in_shape=gdf.geometry.within(gdf.shape_geometry)
    ).query('vp_in_shape==True')
    
    gdf2 = (gdf.groupby("trip_instance_key", 
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
    trip_to_shape = helpers.import_scheduled_trips(
        analysis_date,
        columns = ["trip_instance_key", "shape_array_key"],
        get_pandas = True
    )
    
    shapes = buffer_shapes(analysis_date, buffer_meters=35)        
    
    print("get shapes")

    vp_usable = dd.read_parquet(
        f"{SEGMENT_GCS}vp_usable_{analysis_date}",
        columns=["trip_instance_key", "x", "y"],
    ).merge(
        trip_to_shape,
        on = "trip_instance_key",
        how = "inner"
    ).repartition(npartitions=20)
        
    vp_with_shape = dd.merge(
        vp_usable,
        shapes,
        on = "shape_array_key",
        how = "inner"
    ).persist()
    
    print("merge vp with shape geometry")
    
    results = vp_with_shape.map_partitions(
        count_vp_in_shape,
        meta = {
            "trip_instance_key": "str",
            "vp_in_shape": "int32"},
        align_dataframes = False
    ).persist()
    
    results = results.repartition(npartitions=2)
            
    results.to_parquet(
        f"{RT_SCHED_GCS}vp_trip/intermediate/"
        f"spatial_accuracy_{analysis_date}",
        overwrite = True
    )
    
    return


def attach_operator_natural_identifiers(
    df: pd.DataFrame,
    analysis_date: str
) -> pd.DataFrame:
    """
    Add route id, direction id,
    off_peak, and time_of_day columns. Do some
    light cleaning.
    """
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
    )
    
    return df2


def add_metrics(df: pd.DataFrame) -> pd.DataFrame:
    """
    Add metrics and numeric rounding.
    """
    integrify = ["vp_in_shape", "total_vp"]
    df[integrify] = df[integrify].fillna(0).astype("int")
    
    df = df.assign(
        vp_per_min = df.total_pings / df.rt_service_minutes,
        pct_in_shape = df.vp_in_shape / df.total_vp,
        pct_rt_journey_vp = df.minutes_atleast1_vp / df.rt_service_minutes,
        pct_rt_journey_atleast2_vp = df.minutes_atleast2_vp / df.rt_service_minutes,
        pct_sched_journey_atleast1_vp = (df.minutes_atleast1_vp / 
                                         df.scheduled_service_minutes),
        pct_sched_journey_atleast2_vp = (df.minutes_atleast2_vp / 
                                         df.scheduled_service_minutes),
    )
    
    two_decimal_cols = [
        "vp_per_min", "rt_service_minutes", 
    ]
    
    df[two_decimal_cols] = df[two_decimal_cols].round(2)
    
    three_decimal_cols = [
        c for c in df.columns if "pct_" in c
    ]
    
    df[three_decimal_cols] = df[three_decimal_cols].round(3)

    # Mask percents for any values above 100%
    # Scheduled service minutes can be assumed to be shorter than 
    # RT service minutes, so there can be more minutes with vp data available
    mask_me = [c for c in df.columns if "pct_sched_journey" in c]
    for c in mask_me:
        df[c] = df[c].mask(df[c] > 1, 1)

    return df    


# Complete
def rt_schedule_trip_metrics(
    analysis_date: str, 
    dict_inputs: dict
) -> pd.DataFrame:
    
    start = datetime.datetime.now()
    print(f"Started running script at {start} for {analysis_date} metrics")
    
    """
    Keep for testing temporarily
    operator = "Bay Area 511 Muni VehiclePositions"
    
    gtfs_key = "7cc0cb1871dfd558f11a2885c145d144"
    
    vp_usable = (vp_usable.loc
    [vp_usable.schedule_gtfs_dataset_key ==gtfs_key]
    .reset_index(drop=True))
    """

    basic_counts_by_vp_trip(analysis_date)
    
    time1 = datetime.datetime.now()
    logger.info(f"tabular trip metrics: {time1 - start}")
    '''
    spatial_accuracy_count(analysis_date)
    
    time2 = datetime.datetime.now()
    logger.info(f"spatial trip metrics: {time2 - time1}")
    
    
    ## Merges ##
    rt_service_df = pd.read_parquet(
        f"{RT_SCHED_GCS}vp_trip/intermediate/",
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
    ).pipe(
        add_metrics
    ).pipe(attach_operator_natural_identifiers, analysis_date)
    
    # Save
    TRIP_EXPORT = dict_inputs["trip_metrics"]
    df.to_parquet(f"{RT_SCHED_GCS}{TRIP_EXPORT}_{analysis_date}.parquet")

    time3 = datetime.datetime.now()
    logger.info(f"Total run time for metrics on {analysis_date}: {time3 - start}")
    '''
    
if __name__ == "__main__":
    
    LOG_FILE = "../logs/rt_v_scheduled_trip_metrics.log"
    logger.add(LOG_FILE, retention="3 months")
    logger.add(sys.stderr, 
               format="{time:YYYY-MM-DD at HH:mm:ss} | {level} | {message}", 
               level="INFO")
    
    from shared_utils import rt_dates
    oct_week = rt_dates.get_week("oct2023", exclude_wed=True)
    apr_week = rt_dates.get_week("apr2023", exclude_wed=True)
    for date in oct_week + apr_week:
    #for date in update_vars.trip_analysis_date_list:
        rt_schedule_trip_metrics(date, CONFIG_DICT)
        #print('Done')