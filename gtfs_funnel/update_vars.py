from shared_utils import catalog_utils, rt_dates

all_dates = (rt_dates.y2024_dates + rt_dates.y2023_dates + 
             rt_dates.oct_week + rt_dates.apr_week)

analysis_date_list = [
    rt_dates.DATES["mar2024"]
] 

GTFS_DATA_DICT = catalog_utils.get_catalog("gtfs_analytics_data")