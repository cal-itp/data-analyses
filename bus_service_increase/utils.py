"""
Utility functions
"""
import datetime as dt
import json
import pandas as pd

from calendar import THURSDAY, SATURDAY, SUNDAY
from calitp.storage import get_fs
from pathlib import Path

fs = get_fs()

GCS_PROJECT = "cal-itp-data-infra"
BUCKET_NAME = "calitp-analytics-data"
BUCKET_DIR = "data-analyses/bus_service_increase"
GCS_FILE_PATH = f"gs://{BUCKET_NAME}/{BUCKET_DIR}/"

DATA_PATH = Path("./data/")
IMG_PATH = Path("./img/")

def import_export(DATASET_NAME: str, OUTPUT_FILE_NAME: str, GCS:bool=True): 
    """
    DATASET_NAME: str. Name of csv dataset.
    OUTPUT_FILE_NAME: str. Name of output parquet dataset.
    """
    df = pd.read_csv(f"{DATASET_NAME}.csv")    
    
    if GCS is True:
        df.to_parquet(f"{GCS_FILE_PATH}{OUTPUT_FILE_NAME}.parquet")
    else:
        df.to_parquet(f"./{OUTPUT_FILE_NAME}.parquet")
    

def get_recent_dates() -> dict:
    '''
    Return a dict with dt.date objects for a recent thursday, saturday, and sunday.
    Useful for querying.
    '''

    two_wks_ago = dt.date.today() - dt.timedelta(days=14)
    dates = []
    for day_of_week in [THURSDAY, SATURDAY, SUNDAY]:
        offset = (two_wks_ago.weekday() - day_of_week) % 7
        last_day = two_wks_ago - dt.timedelta(days=offset)
        dates.append(last_day)
    
    return dict(zip(['thurs', 'sat', 'sun'], dates))


# There are multiple feeds, with different trip_keys but same trip_ids
# Only keep calitp_url_number == 0 EXCEPT LA Metro
def include_exclude_multiple_feeds(df: pd.DataFrame, 
                                   id_col: str = "itp_id",
                                   include_ids: list = [182], 
                                   exclude_ids: list = [200]) -> pd.DataFrame:
    """
    df: pandas.DataFrame.
    id_col: str, column name for calitp_itp_id, such as "itp_id" 
    include_ids: list, 
            list of itp_ids that are allowed to have multiple feeds 
            (Ex: LA Metro) 
    exclude_ids: list, list of itp_ids to drop. (Ex: MTC, regional feed)
    """
    # If there are explicit regional feeds to drop, put that in exclude_ids
    group_cols = list(df.columns)
    dup_cols = [i for i in group_cols if i != "calitp_url_number"]

    df2 = (df[~df[id_col].isin(exclude_ids)]
           .sort_values(group_cols)
           .drop_duplicates(subset=dup_cols)
           .reset_index(drop=True)
          )
    
    print(f"# obs in original df: {len(df)}")
    print(f"# obs in new df: {len(df2)}")
    
    # There are still multiple operators here
    # But, seems like for those trip_ids, they are different values 
    # between url_number==0 vs url_number==1
    multiple_urls = list(df2[df2.calitp_url_number==1][id_col].unique())
    print(f"These operators have multiple calitp_url_number values: {multiple_urls}")    
    
    return df2


def fix_gtfs_time(gtfs_timestring: str) -> str:
    '''Reformats a GTFS timestamp (which allows the hour to exceed 24 to 
    mark service day continuity)
    to standard 24-hour time.
    '''
    split = gtfs_timestring.split(':')
    hour = int(split[0])
    if hour >= 24:
        split[0] = str(hour - 24)
        corrected = (':').join(split)
        return corrected.strip()
    else:
        return gtfs_timestring.strip()

    
# https://stackoverflow.com/questions/25052980/use-pickle-to-save-dictionary-in-python
def save_request_json(my_list: list, 
                      name: str, 
                      DATA_PATH: Path = DATA_PATH, 
                      GCS_FILE_PATH: Path = GCS_FILE_PATH):
    """
    Input a json response that comes back as a list.
    Write it to GCS bucket
    """
    # result comes back as a list, but add [0] and it's a dict
    my_dict = my_list[0]
    
    # Convert to json
    #https://gist.github.com/romgapuz/c7a4cedb85f090ac1b55383a58fa572c
    json_obj = json.loads(json.dumps(my_dict, default=str))
    
    # Save json locally
    json.dump(json_obj, open(f"{DATA_PATH}{name}.json", "w", encoding='utf-8'))
    
    # Put the json object in GCS. 
    fs.put(f"{DATA_PATH}{name}.json", f"{GCS_FILE_PATH}{name}.json")
    print(f"Saved {name}")
    
    
def open_request_json(name: str, 
                      DATA_PATH: Path = DATA_PATH, 
                      GCS_FILE_PATH: Path = GCS_FILE_PATH) -> dict:
    """
    Grab the json object from GCS and import it.
    
    Returns a dict.
    """
    # Download object from GCS bucket
    gcs_json = fs.get(f"{GCS_FILE_PATH}{name}.json", f"{DATA_PATH}{name}.json")
    my_dict = json.load(open(f"{DATA_PATH}{name}.json"))
    
    return my_dict