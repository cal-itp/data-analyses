import os
os.environ["CALITP_BQ_MAX_BYTES"] = str(10_000_000_000_000)
os.environ['USE_PYGEOS'] = '0'

import datetime
import pandas as pd
import pandas_gbq

from calitp_data_analysis.tables import tbls
from siuba import *

from shared_utils import gtfs_utils_v2
from segment_speed_utils.project_vars import PREDICTIONS_GCS, analysis_date

def download_stop_time_updates(
    analysis_date: str, 
    operator_name: str
):
    cols = [
        "gtfs_dataset_key", "_gtfs_dataset_name",
        "trip_id",
        "stop_id", "stop_sequence",
        "arrival_time", "departure_time", 
    ]

    START = 25_200_000 # 9am in milliseconds
    #END = 57_600_000 # 4pm in milliseconds 
    df = (tbls.mart_gtfs.fct_stop_time_updates()
          >> filter(_.dt == analysis_date)
          >> filter(_._gtfs_dataset_name == operator_name)
          #>> select(*cols)
          >> collect()
    )
    
    return df


def download_stop_time_updates_pandas(
    analysis_date: str, 
    operator_name: str
):  
    
    df  = pd.read_gbq(
        f"""
        select 
            *
        from `cal-itp-data-infra`.`mart_gtfs`.`fct_stop_time_updates`
        where dt = '{analysis_date}' AND _gtfs_dataset_name = '{operator_name}'
        """, 
        project_id = 'cal-itp-data-infra'
    )
    
    operator_snakecase = snake_case_string(operator_name)
    
    df.to_parquet(
        f"{PREDICTIONS_GCS}stop_time_update_"
        f"{analysis_date}_{operator_snakecase}.parquet")


    
def snake_case_string(string: str):
    return (string.replace('TripUpdates', '')
            .replace('Trip Updates', '')
            .strip()
            .lower()
            .replace(' ', '_')
           )

        
if __name__ == "__main__":
    
    #from dask.distributed import Client
    
    #client = Client("dask-scheduler.dask.svc.cluster.local:8786")
    
    start = datetime.datetime.now()
        
    rt_datasets = gtfs_utils_v2.get_transit_organizations_gtfs_dataset_keys(
        keep_cols=["key", "name", "type", "regional_feed_type"],
        custom_filtering={"type": ["trip_updates"]},
        get_df = True
    ) >> collect()
    
    # Exclude regional feed and precursors
    exclude = ["Bay Area 511 Regional VehiclePositions"]
    rt_datasets = rt_datasets[
        ~(rt_datasets.name.isin(exclude)) & 
        (rt_datasets.regional_feed_type != "Regional Precursor Feed")
    ].reset_index(drop=True)
    
    rt_dataset_names = rt_datasets.name.unique().tolist()
    
    operators_to_download = [
        #"LA DOT", # did not re-download...only has a subset of columns
        "Dumbarton Express",
        "Anaheim Resort",
        "Bay Area 511 Union City",
        "Fairfield and Suisun",
    ]
    matching = [i for i in rt_dataset_names 
                if any(name in i for name in operators_to_download)]
    
    for i in matching:
        download_stop_time_updates_pandas(
            analysis_date,
            i,
        )
       
    end = datetime.datetime.now()
    print(f"execution time: {end - start}")
    
    #client.close()
    
    