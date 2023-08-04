"""
Utility functions
"""
import datetime as dt
import json
import pandas as pd

from calendar import THURSDAY, SATURDAY, SUNDAY

GCS_PROJECT = "cal-itp-data-infra"
BUCKET_NAME = "calitp-analytics-data"
BUCKET_DIR = "data-analyses/bus_service_increase"
GCS_FILE_PATH = f"gs://{BUCKET_NAME}/{BUCKET_DIR}/"

DATA_PATH = "./data/"
IMG_PATH = "./img/"
    

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


# https://stackoverflow.com/questions/25052980/use-pickle-to-save-dictionary-in-python
def save_request_json(my_list: list, 
                      name: str, 
                      data_path: str = DATA_PATH, 
                      gcs_file_path: str = GCS_FILE_PATH):
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
    json.dump(json_obj, open(f"{data_path}{name}.json", "w", encoding='utf-8'))
    
    # Put the json object in GCS. 
    fs.put(f"{data_path}{name}.json", f"{gcs_file_path}{name}.json")
    print(f"Saved {name}")
    
    
def open_request_json(name: str, 
                      data_path: str = DATA_PATH, 
                      gcs_file_path: str = GCS_FILE_PATH) -> dict:
    """
    Grab the json object from GCS and import it.
    
    Returns a dict.
    """
    # Download object from GCS bucket
    gcs_json = fs.get(f"{gcs_file_path}{name}.json", f"{data_path}{name}.json")
    my_dict = json.load(open(f"{data_path}{name}.json"))
    
    return my_dict