from shared_utils import gtfs_utils_v2
from .constants import WAREHOUSE_DATE_STRFTIME, GTFS_DATA_DICT
from .rt_stop_times_copied_functions import assemble_scheduled_rt_stop_times_keep_all_scheduled
import pandas as pd
import datetime as dt

def schedule_feed_name_to_gtfs_dataset_key(feed_name: str) -> str:
    """Utilize gtfs_utils to convert the name of a schedule feed to the corresponding feed key"""
    feed_key = gtfs_utils_v2.schedule_daily_feed_to_gtfs_dataset_name(
        selected_date=SAMPLE_DATE_STR,
        keep_cols=["name", "gtfs_dataset_key"]
    ).set_index("name").at[feed_name, "gtfs_dataset_key"]
    return feed_key

def get_schedule_rt_stop_times_table(gtfs_dataset_key: str, service_date: dt.date | str) -> pd.DataFrame:
    date_str = (
        service_date
        if type(service_date) is not dt.date 
        else service_date.strftime(WAREHOUSE_DATE_STRFTIME)
    )
    #gcs_dir_name = GTFS_DATA_DICT.rt_vs_schedule_tables.dir
    #gcs_table_name = GTFS_DATA_DICT.rt_vs_schedule_tables.schedule_rt_stop_times
    #rt_schedule_stop_times_uri = f"{gcs_dir_name}{gcs_table_name}_{date_str}.parquet"
    #schedule_rt_stop_times = pd.read_parquet(rt_schedule_stop_times_uri)
    schedule_rt_stop_times = assemble_scheduled_rt_stop_times_keep_all_scheduled(
        service_date,
        [*GTFS_DATA_DICT.rt_stop_times.trip_stop_cols]
    )
    schedule_rt_stop_times_single_agency = schedule_rt_stop_times.loc[
        schedule_rt_stop_times["schedule_gtfs_dataset_key"] == gtfs_dataset_key
    ].sort_values(
        ["trip_instance_key", "stop_sequence"]
    )
    return schedule_rt_stop_times_single_agency