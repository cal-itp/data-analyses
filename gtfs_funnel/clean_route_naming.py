"""
Create file with cleaned up route info.
"""
import pandas as pd

from segment_speed_utils import gtfs_schedule_wrangling, helpers
from update_vars import GTFS_DATA_DICT, SCHED_GCS
from shared_utils import rt_dates

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
            columns = ["gtfs_dataset_key", "name", "route_id", 
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


def tag_rapid_express_local(route_name_string: str):
    """
    Use the combined route_name and see if we can 
    tag out words that indicate the route is
    express, rapid, or local.
    
    NACTO typologies are based on combination of roads and
    route characteristics.
    But we aren't able to find out express routes based on their definition.
    Also, a lot of rapid routes are missed.
    """
    route_name_string = route_name_string.lower()
    
    express = 0
    rapid = 0
    local = 0
    
    if "express" in route_name_string:
        express = 1
    if "rapid" in route_name_string:
        rapid = 1
    if "local" in route_name_string:
        local = 1
    
    return pd.Series(
            [express, rapid, local], 
            index=['is_express', 'is_rapid', 'is_local']
        )

if __name__ == "__main__":
    
    CLEANED_ROUTE_NAMING = GTFS_DATA_DICT.schedule_tables.route_identification
    
    date_list = rt_dates.y2024_dates + rt_dates.y2023_dates

    df =  concatenate_routes_across_dates(date_list)
    
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
    
    typology_tags = df2.recent_combined_name.apply(tag_rapid_express_local)
    df3 = pd.concat([df2, typology_tags], axis=1)
    
    df3.to_parquet(f"{SCHED_GCS}{CLEANED_ROUTE_NAMING}.parquet")
