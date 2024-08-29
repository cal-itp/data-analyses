import geopandas as gpd
import pandas as pd

from segment_speed_utils.project_vars import RT_SCHED_GCS, GTFS_DATA_DICT

route_dir_stop_cols = [
    "schedule_gtfs_dataset_key", "name", "time_period",
    "route_id", "direction_id", 
    "stop_pair", "stop_pair_name"
]

operator_list = df = pd.read_parquet(
    f"{RT_SCHED_GCS}{GTFS_DATA_DICT.digest_tables.route_segment_speeds}.parquet",
    columns = ["name"]
).name.unique()

def route_segment_speeds_ts(
    file: str = GTFS_DATA_DICT.digest_tables.route_segment_speeds,
    **kwargs
) -> gpd.GeoDataFrame:
    """
    Subset the concatenated time-series route-segment speeds
    and grab all-day only and a smaller set of columns.
    """
    df = gpd.read_parquet(
        f"{RT_SCHED_GCS}{file}.parquet",
        **kwargs
        #filters = [[("time_period", "==", "all_day")]],
        #columns = route_dir_stop_cols + ["service_date", "p50_mph", "geometry"]
    )
    
    return df


def count_time_series_values_by_route_direction_stop(
    df: gpd.GeoDataFrame,
    group_cols: list
) -> pd.DataFrame:
    """
    For each stop, count how many segment variations are available
    across our time-series.
    """
    df2 = (df
           .groupby(group_cols, group_keys=False)
           .agg({
                "p50_mph": "count",
                "service_date": "nunique",
                "geometry": "nunique"
            }).reset_index()
           .rename(columns = {
                "p50_mph": "n_speed_values",
                "service_date": "n_dates",
                "geometry": "n_geometry"
            })
          )
    
    return df2