from shared_utils import gtfs_utils_v2
from constants import WAREHOUSE_DATE_STRFTIME, GTFS_DATA_DICT
import pandas as pd
import datetime as dt

def schedule_feed_name_to_feed_key(feed_key: str) -> str:
    """Utilize gtfs_utils to convert the name of a schedule feed to the corresponding feed key"""
    feed_key = gtfs_utils_v2.schedule_daily_feed_to_gtfs_dataset_name(
        selected_date=SAMPLE_DATE_STR,
        keep_cols=["name", "gtfs_dataset_key"]
    ).set_index("name").at["Big Blue Bus Schedule", "gtfs_dataset_key"]
    return feed_key

def get_schedule_rt_stop_times_table(feed_key: str, service_date: dt.date | str) -> pd.DataFrame:
    date_str = (
        service_date
        if type(service_date) is not dt.date 
        else service_date.strftime(WAREHOUSE_DATE_STRFTIME)
    )
    gcs_dir_name = GTFS_DATA_DICT.rt_vs_schedule_tables.dir
    gcs_table_name = GTFS_DATA_DICT.rt_vs_schedule_tables.schedule_rt_stop_times
    rt_schedule_stop_times_uri = f"{gcs_dir_name}{gcs_table_name}_{date_str}.parquet"
    schedule_rt_stop_times = pd.read_parquet(rt_schedule_stop_times_uri)
    schedule_rt_stop_times_single_agency = schedule_rt_stop_times.loc[
        schedule_rt_stop_times["schedule_gtfs_dataset_key"] == feed_key
    ].copy()
    return schedule_rt_stop_times_single_agency