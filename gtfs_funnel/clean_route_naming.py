"""
Create file with cleaned up route info.
"""
import pandas as pd

from segment_speed_utils import gtfs_schedule_wrangling, helpers
from update_vars import GTFS_DATA_DICT, SCHED_GCS

def concatenate_routes_across_dates(
    analysis_date_list: list
) -> pd.DataFrame:
    """
    Concatenate all the trip tables to get deduped 
    route info.
    Create a combined_name column that is combination of 
    route_short_name and route_long_name.
    """
    df = pd.concat([
        helpers.import_scheduled_trips(
            analysis_date,
            columns = ["gtfs_dataset_key", "name", 
                       "route_id", 
                       "route_long_name", "route_short_name", "route_desc"],
            get_pandas = True
        ).assign(
            service_date = pd.to_datetime(analysis_date)
        ) for analysis_date in analysis_date_list
    ], axis=0, ignore_index=True)
    
    # Fill in missing values
    df = df.assign(
        route_id = df.route_id.fillna(""),
        route_short_name = df.route_short_name.fillna(""),
        route_long_name = df.route_long_name.fillna(""),
    )

    df = df.assign(
        combined_name = df.route_short_name + "__" + df.route_long_name
    )
    
    return df

if __name__ == "__main__":
    
    from shared_utils.rt_dates import all_dates
    
    CLEANED_ROUTE_NAMING = GTFS_DATA_DICT.schedule_tables.route_identification
    
    df =  concatenate_routes_across_dates(all_dates)
    
    df = df.assign(
        route_id2 = df.apply(
            lambda x: 
            gtfs_schedule_wrangling.standardize_route_id(
                x, "name", "route_id"), 
            axis=1)
    )
    
    route_cols = ["schedule_gtfs_dataset_key", "name", "route_id2"]

    df2 = gtfs_schedule_wrangling.most_recent_route_info(
        df,
        group_cols = route_cols,
        route_col = "combined_name"
    ).pipe(
        gtfs_schedule_wrangling.most_recent_route_info, 
        group_cols = ["schedule_gtfs_dataset_key", "name", 
                      "recent_combined_name"],
        route_col = "route_id2"
    )
    
    df2.to_parquet(f"{SCHED_GCS}{CLEANED_ROUTE_NAMING}.parquet")
