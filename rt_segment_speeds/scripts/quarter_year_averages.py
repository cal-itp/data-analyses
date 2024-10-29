import geopandas as gpd
import pandas as pd

from dask import delayed, compute

from update_vars import SEGMENT_GCS, GTFS_DATA_DICT
from segment_speed_utils import time_series_utils

from average_segment_speeds import concatenate_trip_segment_speeds

def concatenate_single_day_summaries(
    speed_file: str,
    analysis_date_list: list,
    group_cols: list
):
    """
    Concatenate several single day averages
    and we'll take the average over that.
    
    If we have 6 dates of segment p20/p50/p80 speeds,
    we'll treat each date as an independent entity
    and average the p20/p50/p80 speeds over that time period.
    
    We will not go back to trip segment speeds for each date
    and do a weighted average.
    In an extreme case, if one date had 1,000 trips and another date
    had 100 trips, one date would have 10x the weight of another date,
    and here, we just want to see where the p20 speed typically is.
    """
    df = time_series_utils.concatenate_datasets_across_dates(
        SEGMENT_GCS,
        speed_file,
        analysis_date_list,
        data_type  = "df",
        columns = group_cols + ["p20_mph", "p50_mph", "p80_mph", "n_trips"],
        get_pandas = False
    )
    
    df = df.assign(
        year = df.service_date.dt.year,
        quarter = df.service_date.dt.quarter,
    )

    return df

def get_aggregation(df: pd.DataFrame, group_cols: list):
    speed_cols = [c for c in df.columns if "_mph" in c]

    df2 = (df
           .groupby(group_cols, group_keys=False)
           .agg(
               {**{c: "mean" for c in speed_cols},
                "n_trips": "sum"})
           .reset_index()
          )
    
    return df2

if __name__ == "__main__":
    
    from shared_utils import rt_dates

    group_cols = [
        "route_id", "direction_id", 
        "stop_pair", "stop_pair_name", 
        "time_period",
        'name', # do not use schedule_gtfs_dataset_key, which can differ over time
        'caltrans_district', 'organization_source_record_id',
        'organization_name', 'base64_url'
    ] 
    
    FILE = GTFS_DATA_DICT["stop_segments"]["route_dir_single_segment"]

    quarter_df = concatenate_single_day_summaries(
        FILE,
        all_dates,
        group_cols
    ).pipe(get_aggregation, group_cols + ["year", "quarter"])

    quarter_df = compute(quarter_df)[0]
    quarter_df.to_parquet(f"{SEGMENT_GCS}{FILE}_quarter.parquet")
    
    year_df = concatenate_single_day_summaries(
        FILE,
        all_dates,
        group_cols
    ).pipe(get_aggregation, group_cols + ["year"])
    
    year_df = compute(year_df)[0]
    year_df.to_parquet(f"{SEGMENT_GCS}{FILE}_year.parquet")
    