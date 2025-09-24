"""
Simple data wrangling to get route-direction grain
ready for visualization.
Rename columns, format values, 
"""
import datetime
import pandas as pd
import sys

from loguru import logger

from update_vars import GTFS_DATA_DICT, RT_SCHED_GCS

route_direction_cols_for_viz = [
    "direction_id",
    "time_period",
    "avg_scheduled_service_minutes",
    "n_scheduled_trips",
    'n_vp_trips',
    "recent_combined_name",
    "route_primary_direction",
    "minutes_atleast1_vp", 
    "minutes_atleast2_vp",
    "is_early",
    "is_ontime",
    "is_late",
    "vp_per_minute",
    "pct_in_shape",
    "pct_sched_journey_atleast1_vp",
    "pct_sched_journey_atleast2_vp",
    "rt_sched_journey_ratio",
    "speed_mph",
    "analysis_name",
    "headway_in_minutes",
    "sched_rt_category", # added this
    'avg_stop_miles'
]

readable_col_names = {
    "direction_id": "Direction (0/1)",
    "time_period": "Period",
    "avg_scheduled_service_minutes": "Average Scheduled Service (trip minutes)",
    "n_scheduled_trips": "# Scheduled Trips",
    'n_vp_trips': "# Realtime Trips",
    "recent_combined_name": "Route",
    "route_primary_direction": "Direction",
    "minutes_atleast1_vp": "# Minutes with 1+ VP per Minute",
    "minutes_atleast2_vp": "# Minutes with 2+ VP per Minute",
    "is_early": "# Early Arrival Trips",
    "is_ontime": "# On-Time Trips",
    "is_late": "# Late Trips",
    "vp_per_minute": "Average VP per Minute",
    "pct_in_shape": "% VP within Scheduled Shape",
    "pct_sched_journey_atleast1_vp": "% Scheduled Trip w/ 1+ VP/Minute",
    "pct_sched_journey_atleast2_vp": "% Scheduled Trip w/ 2+ VP/Minute",
    "rt_sched_journey_ratio": "Realtime versus Scheduled Service Ratio",
    "speed_mph": "Speed (MPH)",
    "analysis_name": "Analysis Name",
    "headway_in_minutes": "Headway (Minutes)",
    'avg_stop_miles':"Average Stop Distance (Miles)",
    "sched_rt_category":"GTFS Availability",
}

def data_wrangling_for_visualizing(
    df: pd.DataFrame, 
    subset: list, 
    readable_col_names: dict,
    date_col:str,
    qtr_or_date:str,
) -> pd.DataFrame:
    """
    Keep the subset of columns, rename for parameterized notebook.
    """
    # Add onto subset
    subset2 = subset + [date_col]
    
    # create new columns
    df = df.assign(
        headway_in_minutes = 60 / df.frequency
    ).round(0)
    
    # these show up as floats but should be integers
    # also these aren't kept...
    route_typology_cols = [
        f"is_{c}" for c in 
        ["express", "rapid",
         "ferry", "rail", "coverage",
         "local", "downtown_local"]
    ]
    
    float_cols = [c for c in df.select_dtypes(include=["float"]).columns 
                     if c not in route_typology_cols and "pct" not in c]
    
    df[float_cols] = df[float_cols].round(2)
    

    pct_cols = [c for c in df.columns if "pct" in c]
    df[pct_cols] = df[pct_cols].round(0) * 100

    # Add service_date or quarter date to the readable_col_names dictionary
    readable_col_names[date_col] = qtr_or_date
    
    df2 = df.assign(
        time_period = df.time_period.astype(str).str.replace("_", " ").str.title()
    )[subset2].query(
        'sched_rt_category == "schedule_and_vp"'
    ).rename(
        columns = readable_col_names
    ).reset_index(drop=True)

    return df2


if __name__ == "__main__":
    
    logger.add("./logs/digest_data_prep.log", retention="3 months")
    logger.add(sys.stderr, 
               format="{time:YYYY-MM-DD at HH:mm:ss} | {level} | {message}", 
               level="INFO")
    
    start = datetime.datetime.now()
    
    ROUTE_DIR_MONTH_FILE = GTFS_DATA_DICT.digest_tables.monthly_route_schedule_vp
    ROUTE_DIR_MONTH_REPORT = GTFS_DATA_DICT.digest_tables.monthly_route_schedule_vp_report
    
    ROUTE_DIR_QTR_FILE = GTFS_DATA_DICT.digest_tables.quarterly_route_schedule_vp
    ROUTE_DIR_QTR_EXPORT = GTFS_DATA_DICT.digest_tables.quarterly_route_schedule_vp_report
    
    # Report by month
    # Groupby recent combined name + portfolio org name + service date for _report 
    month_route_dir_df = pd.read_parquet(
        f"{RT_SCHED_GCS}{ROUTE_DIR_MONTH_FILE}.parquet"
    )
    month_route_dir_df2 =  data_wrangling_for_visualizing(month_route_dir_df,
                                                         route_direction_cols_for_viz,
                                                         readable_col_names,
                                                        "service_date",
                                                        "Date")
    month_route_dir_df2.to_parquet(
        f"{RT_SCHED_GCS}{ROUTE_DIR_MONTH_REPORT}.parquet"
    )
    
    # Take the group by around 130 and aggregate up to quarterly
    qtr_route_dir_df = pd.read_parquet(
        f"{RT_SCHED_GCS}{ROUTE_DIR_QTR_FILE}.parquet"
    )
    
    qtr_route_dir_df2 =  data_wrangling_for_visualizing(qtr_route_dir_df,
                                                         route_direction_cols_for_viz,
                                                         readable_col_names,
                                                        "year_quarter",
                                                        "Quarter")
    qtr_route_dir_df2.to_parquet(
        f"{RT_SCHED_GCS}{ROUTE_DIR_QTR_EXPORT}.parquet"
    )
    
    end = datetime.datetime.now()
    logger.info(f"route-direction viz data prep: {end - start}")
