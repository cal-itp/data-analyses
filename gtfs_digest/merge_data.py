import geopandas as gpd
import pandas as pd

from segment_speed_utils.project_vars import SEGMENT_GCS, RT_SCHED_GCS, SCHED_GCS

route_time_cols = ["schedule_gtfs_dataset_key", 
                   "route_id", "direction_id", "time_period"]

def concatenate_schedule_by_route_direction(
    date_list: list
) -> pd.DataFrame:
    """
    Concatenate schedule data that's been 
    aggregated to route-direction-time_period.
    """
    df = pd.concat([
        pd.read_parquet(
            f"{RT_SCHED_GCS}schedule_route_dir/"
            f"schedule_route_direction_metrics_{d}.parquet",
            columns = route_time_cols + [
                "avg_sched_service_min", 
                "avg_stop_meters",
                "n_trips", "frequency",
            ]
        ).assign(
            service_date = pd.to_datetime(d)
        ).astype({"direction_id": "Int64"}) 
        for d in date_list
    ], axis=0, ignore_index=True)
    
    return df


def concatenate_segment_speeds_by_route_direction(
    date_list: list
) -> gpd.GeoDataFrame:
    """
    Concatenate segment speeds data that's been 
    aggregated to route-direction-time_period.
    """
    df = pd.concat([
        gpd.read_parquet(
            f"{SEGMENT_GCS}rollup_singleday/"
            f"speeds_route_dir_segments_{d}.parquet",
            columns = route_time_cols + ["p20_mph", "p50_mph", "p80_mph"]
        ).assign(
            service_date = pd.to_datetime(d)
        ).astype({"direction_id": "Int64"})  
         for d in date_list], 
        axis=0, ignore_index=True
    )
    
    return df


def concatenate_speeds_by_route_direction(
    date_list: list
) -> pd.DataFrame: 
    df = pd.concat([
        pd.read_parquet(
            f"{SEGMENT_GCS}rollup_singleday/"
            f"speeds_route_dir_{d}.parquet",
            columns = route_time_cols + ["speed_mph"]
        ).assign(
            service_date = pd.to_datetime(d)
        ).astype({"direction_id": "Int64"})  
         for d in date_list], 
        axis=0, ignore_index=True
    )
    
    return df


def merge_in_standardized_route_names(df: pd.DataFrame) -> pd.DataFrame:
    standardized_route_names = pd.read_parquet(
        f"{SCHED_GCS}standardized_route_ids.parquet",
        columns = ["schedule_gtfs_dataset_key", "name", 
                   "route_id", "service_date",
                   "recent_route_id2", "recent_combined_name"
                  ]
    )
    
    df = pd.merge(
        df,
        standardized_route_names,
        on = ["schedule_gtfs_dataset_key", "route_id", "service_date"],
        how = "left",
    )
    
    df = df.assign(
        route_short_name = (df.recent_combined_name
                            .str.split("__", expand=True)[0]),
        route_long_name = (df.recent_combined_name
                           .str.split("__", expand=True)[1]),
    ).drop(
        columns = ["route_id", "recent_combined_name"]
    ).rename(
        columns = {"recent_route_id2": "route_id"}
    )
    
    return df


def clean_up_for_charts(df: pd.DataFrame) -> pd.DataFrame:
    # Clean up, round columns, get it as close to ready for charts
    df = df.assign(
        direction_id = df.direction_id.astype("int"),
        avg_sched_service_min = df.avg_sched_service_min.round(1),
        avg_stop_meters = df.avg_stop_meters.round(1),
    )

    return df


if __name__ == "__main__":
    
    from shared_utils.rt_dates import y2023_dates, y2024_dates
    
    analysis_date_list = y2024_dates + y2023_dates 
    
    df_schedule = concatenate_schedule_by_route_direction(analysis_date_list)
    df_avg_speeds = concatenate_speeds_by_route_direction(analysis_date_list)
    
    df_sched_speeds = pd.merge(
        df_schedule,
        df_avg_speeds,
        on = route_time_cols + ["service_date"],
        how = "outer",
        indicator = "sched_rt_category"
    )
    
    category_dict = {
        "left_only": "schedule_only",
        "both": "schedule_and_vp",
        "right_only": "vp_only"
    }
    
    df_sched_speeds= df_sched_speeds.assign(
        sched_rt_category = df_sched_speeds.sched_rt_category.map(category_dict)
    ).pipe(merge_in_standardized_route_names)

    
    df_sched_speeds.to_parquet(
        f"{RT_SCHED_GCS}digest/schedule_vp_metrics.parquet"
    )