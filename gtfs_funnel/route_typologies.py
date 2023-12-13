"""
Add some GTFS schedule derived metrics
by trip (service frequency and stop spacing).
"""
import geopandas as gpd
import intake
import pandas as pd

from calitp_data_analysis.geography_utils import WGS84
from calitp_data_analysis import utils
from segment_speed_utils import helpers, gtfs_schedule_wrangling, sched_rt_utils
from segment_speed_utils.project_vars import RT_SCHED_GCS, PROJECT_CRS

catalog = intake.open_catalog(
    "../_shared_utils/shared_utils/shared_data_catalog.yml")

def assemble_scheduled_trip_metrics(analysis_date: str):
    
    df = gpd.read_parquet(
        f"{RT_SCHED_GCS}stop_times_direction_{analysis_date}.parquet"
    )

    trips_to_route = helpers.import_scheduled_trips(
        analysis_date,
        columns = ["trip_instance_key", "route_id", "direction_id"],
        get_pandas = True
    )
    
    time_of_day = (sched_rt_utils.get_trip_time_buckets(analysis_date)   
                   [["trip_instance_key", "time_of_day", 
                     "service_minutes"]]
                   .rename(columns = {"service_minutes": "sched_service_min"})
              )
    
    trip_cols = ["schedule_gtfs_dataset_key", "trip_instance_key"]
    
    grouped_df = df.groupby(trip_cols, observed=True, group_keys=False)

    # Get median / mean stop meters for the trip
    # Attach time-of-day and route_id and direction_id
    df2 = pd.merge(
        grouped_df.agg({"stop_meters": "median"}).reset_index().rename(
            columns = {"stop_meters": "median_stop_meters"}),
        time_of_day,
        on = "trip_instance_key",
        how = "left"
    ).merge(
        trips_to_route,
        on = "trip_instance_key",
        how = "inner"
    )
    
    df2 = df2.assign(
        median_stop_meters = df2.median_stop_meters.round(2)
    )
    
    return df2
    
    
def schedule_metrics_by_route_direction(
    df: pd.DataFrame,
    analysis_date: str,
    group_cols: list
) -> pd.DataFrame:
    """
    Aggregate trip-level metrics to route-direction, and 
    attach shape geometry for common_shape_id.
    """
    
    service_freq_df = gtfs_schedule_wrangling.aggregate_time_of_day_to_peak_offpeak(
        df, group_cols)
    
    metrics_df = (df.groupby(group_cols, observed=True, group_keys=False)
                  .agg({
                      "median_stop_meters": "mean", 
                      # take mean of the median stop spacing for trip
                      # does this make sense?
                      # median is the single boiled down metric at the trip-level
                      "sched_service_min": "mean",
                  }).reset_index()
                  .rename(columns = {
                      "median_stop_meters": "avg_stop_meters",
                      "sched_service_min": "avg_sched_service_min"
                  })
                 )
    
    common_shape_for_route_dir = add_common_shape(analysis_date)
    
    df = pd.merge(
        common_shape_for_route_dir,
        metrics_df,
        on = group_cols,
        how = "inner"
    ).merge(
        service_freq_df,
        on = group_cols,
        how = "inner"
    )
    
    return df


def add_common_shape(analysis_date: str):
    """
    For route-direction df, add common_shape_id (most frequent shape)
    and attach that shape geometry
    """
    common_shape = sched_rt_utils.most_common_shape_by_route_direction(analysis_date)
    
    shapes = helpers.import_scheduled_shapes(
        analysis_date, 
        columns = ["shape_array_key", "geometry"],
        crs = WGS84,
        get_pandas = True
    ).pipe(helpers.remove_shapes_outside_ca)
    
    shapes_with_geom = pd.merge(
        shapes,
        common_shape,
        on = "shape_array_key",
        how = "inner"
    )
    
    return shapes_with_geom
    
    
def pop_density_by_shape(shape_df: gpd.GeoDataFrame):
    
    shape_df = shape_df.to_crs(PROJECT_CRS)
    shape_df = shape_df.assign(
        geometry = shape_df.geometry.buffer(5)
    )
    
    calenviroscreen_lehd = (catalog.calenviroscreen_lehd_by_tract.read()
                            [["Tract", "pop_sq_mi", "geometry"]]
                            .to_crs(PROJECT_CRS)
                           )
    
    calenviroscreen_lehd = calenviroscreen_lehd.assign(
        dense = calenviroscreen_lehd.apply(
            lambda x: 1 if x.pop_sq_mi >= 10_000
            else 0, axis=1
        )
    )
    
    group_cols = ["shape_array_key"]

    shape_overlay = gpd.overlay(
        shape_df[group_cols + ["geometry"]].drop_duplicates(), 
        calenviroscreen_lehd, 
        how = "intersection"
    )
    
    shape_overlay = shape_overlay.assign(
        length = shape_overlay.geometry.length,
    )
    
    shape_grouped = (shape_overlay.groupby(group_cols + ["dense"], 
                                          observed=True, group_keys=False)
                 .agg({"length": "sum"})
                 .reset_index()
                 .rename(columns = {"length": "overlay_length"})
                )
    
    shape_grouped = shape_grouped.assign(
        length = (shape_grouped.groupby(group_cols, 
                                        observed=True, group_keys=False)
                  .overlay_length
                  .transform("sum")
                 )
    )
    
    shape_grouped = shape_grouped.assign(
        pct_dense = (shape_grouped.overlay_length
                     .divide(shape_grouped.length)
                     .round(3)
                    )
    ).query('dense == 1')[
        ["shape_array_key", "pct_dense"]
    ].reset_index(drop=True)
    
    return shape_grouped
    
    
if __name__ == "__main__":
    
    from update_vars import analysis_date_list, CONFIG_DICT
    
    TRIP_EXPORT = CONFIG_DICT["trip_metrics_file"]
    ROUTE_DIR_EXPORT = CONFIG_DICT["route_direction_metrics_file"]
    
    for date in analysis_date_list:
        trip_metrics = assemble_scheduled_trip_metrics(date)
        
        trip_metrics.to_parquet(
            f"{RT_SCHED_GCS}{TRIP_EXPORT}_{date}.parquet")
        
        route_cols = [
            "schedule_gtfs_dataset_key", 
            "route_id", 
            "direction_id"
        ]
        
        route_dir_metrics = schedule_metrics_by_route_direction(
            trip_metrics, date, route_cols)
        
        pop_density_df = pop_density_by_shape(route_dir_metrics)
        
        route_dir_metrics2 = pd.merge(
            route_dir_metrics,
            pop_density_df,
            on = "shape_array_key",
            how = "left"
        )
            
        utils.geoparquet_gcs_export(
            route_dir_metrics2,
            RT_SCHED_GCS,
            f"{ROUTE_DIR_EXPORT}_{date}"
        )
