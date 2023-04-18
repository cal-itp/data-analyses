import os
os.environ["CALITP_BQ_MAX_BYTES"] = str(10_000_000_000_000)
os.environ['USE_PYGEOS'] = '0'

#import dask_bigquery

from calitp_data_analysis.tables import tbls
from siuba import *
from segment_speed_utils.project_vars import PREDICTIONS_GCS, analysis_date
from download_stop_time_updates import snake_case_string


dumbarton_url = "aHR0cHM6Ly9hcGkuNTExLm9yZy90cmFuc2l0L3RyaXB1cGRhdGVzP2FnZW5jeT1ERQ=="
anaheim_url = "aHR0cHM6Ly9hcnQudHJpcHNob3QuY29tL3YxL2d0ZnMvcmVhbHRpbWUvdHJpcFVwZGF0ZS9DQTU1OEREQy1EN0YyLTRCNDgtOUNBQy1ERUVBMTEzNEY4MjA="
fairfield_suisun_url = "aHR0cHM6Ly9hcGkuNTExLm9yZy90cmFuc2l0L3RyaXB1cGRhdGVzP2FnZW5jeT1GUw=="

URLS = {
    #dumbarton_url: "Bay Area 511 Dumbarton Express", 
    anaheim_url: "Anaheim Resort",
    #fairfield_suisun_url: "Bay Area 511 Fairfield and Suisun Transit"
}


def stop_time_updates(analysis_date: str, url: str, name: str):
    '''
    ddf = dask_bigquery.read_gbq(
        project_id="cal-itp-data-infra",
        dataset_id="mart_ad_hoc",
        table_id=("fct_final_trip_updates_arrival_departure_times_"
                  "by_trip_stop_20230315_to_20230321"),
        row_filter = f"base64_url = '{url}', service_date = '{analysis_date}'"
    )
    
    ddf.to_parquet(f"{PREDICTIONS_GCS}final_trip_updates_{url}_{analysis_date}")
    '''
    df = (
        tbls.mart_ad_hoc.fct_stop_time_updates_20230315_to_20230321()
        >> filter(_.base64_url == url)
        >> filter(_.service_date == analysis_date)
        >> collect()
    )
    
    operator_name = snake_case_string(name)
    
    df.to_parquet(
        f"{PREDICTIONS_GCS}stop_time_update_"
        f"{analysis_date}_{operator_name}.parquet")
    

def actual_arrival_times_by_trip_stop(analysis_date: str, url: str, name: str):
    '''
    ddf = dask_bigquery.read_gbq(
        project_id="cal-itp-data-infra",
        dataset_id="mart_ad_hoc",
        table_id=("fct_final_trip_updates_arrival_departure_times_"
                  "by_trip_stop_20230315_to_20230321"),
        row_filter = f"base64_url = '{url}', service_date = '{analysis_date}'"
    )
    
    ddf.to_parquet(f"{PREDICTIONS_GCS}final_trip_updates_{url}_{analysis_date}")
    '''
    df = (
        tbls.mart_ad_hoc.fct_final_trip_updates_arrival_departure_times_by_trip_stop_20230315_to_20230321()
        >> filter(_.base64_url == url)
        >> filter(_.service_date == analysis_date)
        >> collect()
    )
    
    operator_name = snake_case_string(name)
    df.to_parquet(
        f"{PREDICTIONS_GCS}final_trip_updates_"
        f"{analysis_date}_{operator_name}.parquet")
    


if __name__ == "__main__":
    
    for url, name in URLS.items():
        stop_time_updates(analysis_date, url, name)
        actual_arrival_times_by_trip_stop(analysis_date, url, name)