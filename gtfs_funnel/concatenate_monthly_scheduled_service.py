"""
Concatenate downloaded monthly scheduled service 
table across years.

Attach organization_source_record_id and 
schedule_gtfs_dataset_key.
"""
import pandas as pd
from segment_speed_utils import helpers, time_helpers, time_series_utils
from segment_speed_utils.project_vars import SCHED_GCS
from shared_utils import rt_dates

if __name__ == "__main__":
    
    from update_vars import CONFIG_DICT
    MONTHLY_SERVICE = CONFIG_DICT["monthly_scheduled_service_file"]

    year_list = [2023, 2024]
    analysis_date_list = (rt_dates.y2024_dates + 
                          rt_dates.y2023_dates + 
                          rt_dates.oct_week + 
                          rt_dates.apr_week)
    
    df = pd.concat(
        [pd.read_parquet(
            f"{SCHED_GCS}scheduled_service_by_route_{y}.parquet") 
         for y in year_list], 
        axis=0, ignore_index=True
    ).rename(columns = {
        "source_record_id": "schedule_source_record_id"
    })

    df = df.assign(
        day_name = df.day_type.map(time_helpers.DAY_TYPE_DICT)
    )
    
    crosswalk = time_series_utils.concatenate_datasets_across_dates(
        SCHED_GCS,
        f"crosswalk/gtfs_key_organization",
        analysis_date_list,
        data_type = "df",
        columns = [
            "schedule_source_record_id", 
            "schedule_gtfs_dataset_key",
            "organization_source_record_id", "organization_name", 
        ],
    ).drop(
        columns = "service_date"
    ).drop_duplicates().reset_index(drop=True)

    df2 = pd.merge(
        df,
        crosswalk,
        on = "schedule_source_record_id",
        how = "inner",
    )
    
    df2.to_parquet(
        f"{SCHED_GCS}{MONTHLY_SERVICE}.parquet"
    ) 