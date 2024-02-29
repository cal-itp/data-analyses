import dask
import dask.dataframe as dd
import geopandas as gpd
import pandas as pd
from calitp_data_analysis.geography_utils import WGS84
import update_vars
from update_vars import route_analysis_date_list, CONFIG_DICT
from segment_speed_utils.project_vars import (
    PROJECT_CRS,
    SEGMENT_GCS,
    RT_SCHED_GCS,
    analysis_date,
)
from segment_speed_utils import gtfs_schedule_wrangling, helpers, wrangle_shapes
import rt_v_scheduled_trip

# Times
import datetime
import sys
from loguru import logger

# cd rt_segment_speeds && pip install -r requirements.txt && cd ../_shared_utils && make setup_env
# cd ../rt_scheduled_v_ran/scripts && python rt_v_scheduled_routes.py

def generate_date(analysis_date:list)->str:
    """
    Manipulate the list of analysis dates
    into a string that can go into a file name
    """
    if len(analysis_date) >= 2:
        first_date = analysis_date[0]
        last_date = analysis_date[-1]
        my_string = first_date.replace('-', '_') + '_to_' + last_date.replace('-', '_')
        return my_string
    else:
        return analysis_date[0]
    
def concatenate_trip_segment_speeds(analysis_date_list: list) -> pd.DataFrame:
    """
    Concatenate the trip parquets together,
    whether it's for single day or multi-day averages.
    """
    TRIP_EXPORT = CONFIG_DICT["trip_metrics"]
    df = pd.concat(
        [
            pd.read_parquet(
                f"{RT_SCHED_GCS}{TRIP_EXPORT}/trip_{analysis_date}.parquet"
            ).assign(service_date=pd.to_datetime(analysis_date))
            for analysis_date in analysis_date_list
        ],
        axis=0,
        ignore_index=True,
    )
    return df

def route_metrics(analysis_date_list: list) -> pd.DataFrame:
  
    df = concatenate_trip_segment_speeds(analysis_date_list)
    
    # Delete out trip generated metrics
    del_cols = [
        "pings_per_min",
        "spatial_accuracy_pct",
        "rt_w_gtfs_pct",
        "rt_v_scheduled_time_pct",
    ]

    df = df.drop(columns=del_cols)

    # Add weighted metrics
    sum_cols = [
        "total_min_w_gtfs",
        "rt_service_min",
        "total_pings",
        "service_minutes",
        "total_vp",
        "vp_in_shape",
    ]

    count_cols = ["trip_instance_key"]

    all_day_groups = ["schedule_gtfs_dataset_key",
                  "route_id",
                  "direction_id",
                  ]
    
    all_day_df = (
        df.groupby(all_day_groups)
        .agg({**{e: "sum" for e in sum_cols}, **{e: "count" for e in count_cols}})
        .reset_index()
    )

    all_day_df = all_day_df.rename(columns={"trip_instance_key": "n_trips"})
    all_day_df =  rt_v_scheduled_trip.add_metrics(all_day_df)
    all_day_df['time_period'] = "all_day"
    
    peak_groups = ["peak_offpeak"] + all_day_groups
    peak_df = (
        df.groupby(peak_groups)
        .agg({**{e: "sum" for e in sum_cols}, **{e: "count" for e in count_cols}})
        .reset_index()
    )

    peak_df = peak_df.rename(columns={"trip_instance_key": "n_trips",'peak_offpeak':'time_period'})
    peak_df =  rt_v_scheduled_trip.add_metrics(peak_df)
    
    final_df = pd.concat([peak_df, all_day_df])
    
    # Save
    analysis_date_file = generate_date(analysis_date_list)
    ROUTE_EXPORT = CONFIG_DICT["route_direction_metrics"]
    final_df.to_parquet(f"{RT_SCHED_GCS}{ROUTE_EXPORT}/trip_{analysis_date_file}.parquet")
    
    return final_df

if __name__ == "__main__":
    route_metrics(update_vars.route_analysis_date_list)
    print('Done')