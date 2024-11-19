"""
Average segment speeds over longer time periods,
a quarter or a year.
"""
import dask.dataframe as dd
import geopandas as gpd
import pandas as pd

from dask import delayed, compute

from calitp_data_analysis import utils
from update_vars import SEGMENT_GCS, GTFS_DATA_DICT

def segment_speeds_one_day(
    segment_type: str,
    analysis_date: list,
    segment_cols: list,
    org_cols: list
):
    """
    Concatenate segment geometry (from rt_segment_speeds)
    for all the dates we have 
    and get it to route-direction-segment grain
    """
    speed_file = GTFS_DATA_DICT[segment_type]["route_dir_single_segment"]
    segment_file = GTFS_DATA_DICT[segment_type]["segments_file"]
    
    speeds_df = pd.read_parquet(
        f"{SEGMENT_GCS}{speed_file}_{analysis_date}.parquet",
        columns = segment_cols + org_cols + [
            "schedule_gtfs_dataset_key",
            "p20_mph", "p50_mph", "p80_mph", "n_trips"]
    ).assign(
        service_date = pd.to_datetime(analysis_date)
    )
    
    segment_gdf = gpd.read_parquet(
        f"{SEGMENT_GCS}{segment_file}_{analysis_date}.parquet",
        columns = segment_cols + [
            "schedule_gtfs_dataset_key", "geometry"]
    ).drop_duplicates().reset_index(drop=True)

    merge_cols = [c for c in speeds_df.columns if c in segment_gdf.columns]
    
    df = pd.merge(
        segment_gdf[merge_cols + ["geometry"]].drop_duplicates(),
        speeds_df,
        on = merge_cols,
        how = "inner"
    )
    
    df = df.assign(
        year = df.service_date.dt.year,
        quarter = df.service_date.dt.quarter,
    )
    
    return df


def get_aggregation(df: pd.DataFrame, group_cols: list):
    """
    Aggregating across days, take the (mean)p20/p50/p80 speed
    and count number of trips across those days.
    """
    speed_cols = [c for c in df.columns if "_mph" in c]

    df2 = (df
           .groupby(group_cols, group_keys=False)
           .agg(
               {**{c: "mean" for c in speed_cols},
                "n_trips": "sum"})
           .reset_index()
          )
    
    return df2

def average_by_time(date_list: list, time_cols: list):
    """
    """
    # These define segments, it's route-dir-stop_pair
    segment_stop_cols = [
        "route_id", "direction_id", 
        "stop_pair", 
    ]
    
    # These are the other columns we need, from speeds, but not in segments
    org_cols = [
        "stop_pair_name",
        "time_period",
        "name",
        'caltrans_district', 'organization_source_record_id',
        'organization_name', 'base64_url'
    ]
    
    delayed_dfs = [
        delayed(segment_speeds_one_day)(
            "stop_segments", 
            one_date,
            segment_stop_cols,
            org_cols
       ) for one_date in date_list
    ]
    
    ddf = dd.from_delayed(delayed_dfs)
    
    group_cols = [
        c for c in segment_stop_cols + org_cols 
        if c not in ["schedule_gtfs_dataset_key"]
    ] + time_cols
    
    speed_averages = get_aggregation(ddf, group_cols)
    speed_averages = speed_averages.compute()
    
    segment_geom = ddf[
        ["name", "geometry"] + segment_stop_cols + time_cols
    ].drop_duplicates().compute()
    
    speed_gdf = pd.merge(
        segment_geom,
        speed_averages,
        on = ["name"] + segment_stop_cols + time_cols,
        how = "inner"
    )
    
    return speed_gdf


if __name__ == "__main__":
    import datetime
    from shared_utils import rt_dates

    segment_type = "stop_segments"
    EXPORT = GTFS_DATA_DICT[segment_type]["route_dir_multi_segment"]
    all_dates = rt_dates.y2024_dates + rt_dates.y2023_dates
    '''
    # quarter averages take x min
    speeds_by_quarter = average_by_time(all_dates, ["year", "quarter"])

    utils.geoparquet_gcs_export(
        speeds_by_quarter,
        SEGMENT_GCS,
        f"{EXPORT}_quarter"
    )
    del speeds_by_quarter
    '''
    # year averages take 14 min
    t0 = datetime.datetime.now()
    speeds_by_year = average_by_time(all_dates, ["year"])

    utils.geoparquet_gcs_export(
        speeds_by_year,
        SEGMENT_GCS,
        f"{EXPORT}_year"
    )    
    t1 = datetime.datetime.now()
    print(f"execution: {t1 - t0}")