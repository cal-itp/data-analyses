"""
Functions for creating profiles for
transit operators.
~0.5 min per date.
"""
import datetime
import geopandas as gpd
import pandas as pd

from calitp_data_analysis import utils
from segment_speed_utils import gtfs_schedule_wrangling, helpers
from segment_speed_utils.project_vars import SCHED_GCS
from shared_utils.rt_utils import METERS_PER_MILE

def schedule_stats_by_operator(
    analysis_date: str,
    group_cols: list = ["schedule_gtfs_dataset_key"]
) -> pd.DataFrame:
    """
    Get operator-level aggregations for n_trips,
    n_routes, n_shapes, n_stops, n_arrivals.
    """
    trips = helpers.import_scheduled_trips(
        analysis_date,
        columns = ["gtfs_dataset_key", "route_id",
                  "trip_instance_key", "shape_array_key"],
        get_pandas = True
    )
    
    stop_times = helpers.import_scheduled_stop_times(
        analysis_date,
        columns = ["schedule_gtfs_dataset_key", 
                   "trip_instance_key", "stop_id"],
        with_direction = True,
        get_pandas = True
    )
    
    nunique_cols = [
        "route_id", "trip_instance_key", "shape_array_key"
    ]
    trip_stats = (trips
                  .groupby(group_cols, 
                           observed=True, group_keys=False)
                  .agg({
                      **{c: "nunique" for c in nunique_cols}
                  }).reset_index()
                  .rename(columns = {
                      "route_id": "operator_n_routes",
                      "trip_instance_key": "operator_n_trips",
                      "shape_array_key": "operator_n_shapes",
                  })
                 )
    
    stop_time_stats = (stop_times
                       .groupby(group_cols, 
                                observed=True, group_keys=False)
                       .agg({
                           "stop_id": "nunique",
                           "trip_instance_key": "count"
                       }).reset_index()
                       .rename(columns = {
                           "stop_id": "operator_n_stops",
                           "trip_instance_key": "operator_n_arrivals"
                       })
    )
    
    longest_shape = longest_shape_by_route(analysis_date)
    shape_stats = (longest_shape
                   .groupby(group_cols, 
                            observed=True, group_keys=False)
                   .agg({"route_length_miles": "sum"})
                   .reset_index()
                   .rename(columns = {
                       "route_length_miles": "operator_route_length_miles"})
                  )
    
    df = pd.merge(
        trip_stats,
        stop_time_stats,
        on = group_cols,
        how = "inner"
    ).merge(
        shape_stats,
        on = group_cols,
        how = "inner"
    )
    
    df = df.assign(
        operator_arrivals_per_stop = df.operator_n_arrivals.divide(
            df.operator_n_stops).round(2)
    )
    
    return df

    
def longest_shape_by_route(analysis_date: str) -> gpd.GeoDataFrame: 
    """
    Start with the longest shape for route-direction.
    Drop duplicates by route to keep longest shape per route.
    This will serve as our metric for operator's service area.
    Keep as gdf so we can plot.
    """
    route_cols = ["schedule_gtfs_dataset_key", "route_id"]
    
    gdf = gtfs_schedule_wrangling.longest_shape_by_route_direction(
        analysis_date
    ).sort_values(
        route_cols + ["route_length"], 
        ascending=[True for i in route_cols] + [False]
    ).drop_duplicates(subset=route_cols).reset_index(drop=True)
    
    gdf = gdf.assign(
        route_length_miles = gdf.route_length.divide(METERS_PER_MILE).round(2)
    )
    
    return gdf


def operator_typology_breakdown(df:pd.DataFrame) -> pd.DataFrame:
    """
    Get a count of how many routes (not route-dir) 
    have a certain primary typology.
    """    
    df2 = (df
           .groupby(
               ["schedule_gtfs_dataset_key", "primary_typology"])
            .agg({"route_id": "nunique"})
            .reset_index()
    )
    
    df_wide = df2.pivot(
        index="schedule_gtfs_dataset_key", 
        columns = "primary_typology",
        values="route_id"
    ).reset_index().fillna(0)
    
    typology_values = ["downtown_local", "local",
                       "rapid", "coverage"]
    
    df_wide[typology_values] = df_wide[typology_values].astype(int)
    
    rename_dict = {old_name: f"n_{old_name}_routes" 
                   for old_name in typology_values}
    df_wide = df_wide.rename(columns = rename_dict)
    
    return df_wide


if __name__ == "__main__":
    
    from update_vars import CONFIG_DICT, analysis_date_list
    
    ROUTE_TYPOLOGY = CONFIG_DICT["route_typologies_file"]
    OPERATOR_EXPORT = CONFIG_DICT["operator_scheduled_stats_file"]
    OPERATOR_ROUTE_EXPORT = CONFIG_DICT["operator_route_file"]
    
    for analysis_date in analysis_date_list:
        start = datetime.datetime.now()
        
        crosswalk = helpers.import_schedule_gtfs_key_organization_crosswalk(
            analysis_date
        )[["schedule_gtfs_dataset_key", 
           "name", "organization_source_record_id", "organization_name"]]
        
        route_typology = pd.read_parquet(
            f"{SCHED_GCS}{ROUTE_TYPOLOGY}_{analysis_date}.parquet"
        )
           
        operator_typology_counts = operator_typology_breakdown(route_typology)
    
        stats = schedule_stats_by_operator(
            analysis_date,
            group_cols = ["schedule_gtfs_dataset_key"]
        ).merge(
            operator_typology_counts,
            on = "schedule_gtfs_dataset_key",
            how = "inner"
        ).merge(
            crosswalk,
            on = "schedule_gtfs_dataset_key",
            how = "inner"
        )
        
        stats.to_parquet(
            f"{SCHED_GCS}{OPERATOR_EXPORT}_{analysis_date}.parquet"
        )
        
        route_gdf = longest_shape_by_route(
            analysis_date
        ).merge(
            crosswalk,
            on = "schedule_gtfs_dataset_key",
            how = "inner"
        )

        utils.geoparquet_gcs_export(
            route_gdf,
            SCHED_GCS,
            f"{OPERATOR_ROUTE_EXPORT}_{analysis_date}"
        )
        
        end = datetime.datetime.now()
        print(f"operator stats for {analysis_date}: {end - start}")
