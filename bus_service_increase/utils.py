"""
Utility functions
"""
import datetime as dt
import gcsfs
import pandas as pd

from calendar import THURSDAY, SATURDAY, SUNDAY

GCS_PROJECT = "cal-itp-data-infra"
BUCKET_NAME = "calitp-analytics-data"
BUCKET_DIR = "data-analyses/bus_service_increase"
GCS_FILE_PATH = f"gs://{BUCKET_NAME}/{BUCKET_DIR}/"

DATA_PATH = "./data/"
IMG_PATH = "./img/"

def import_export(DATASET_NAME, OUTPUT_FILE_NAME, GCS=True): 
    """
    DATASET_NAME: str. Name of csv dataset.
    OUTPUT_FILE_NAME: str. Name of output parquet dataset.
    """
    df = pd.read_csv(f"{DATASET_NAME}.csv")    
    
    if GCS is True:
        df.to_parquet(f"{GCS_FILE_PATH}{OUTPUT_FILE_NAME}.parquet")
    else:
        df.to_parquet(f"./{OUTPUT_FILE_NAME}.parquet")
    

def get_recent_dates():
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
def include_exclude_multiple_feeds(df, id_col = "itp_id",
                                   include_ids = [182], exclude_ids = [200]):
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


def fix_gtfs_time(gtfs_timestring):
    '''Reformats a GTFS timestamp (which allows the hour to exceed 24 to mark service day continuity)
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