"""
Utility functions
"""
import json
import pandas as pd

GCS_PROJECT = "cal-itp-data-infra"
BUCKET_NAME = "calitp-analytics-data"
BUCKET_DIR = "data-analyses/facilities_services"
GCS_FILE_PATH = f"gs://{BUCKET_NAME}/{BUCKET_DIR}/"

DATA_PATH = "./data/"

def save_request_json(my_dict, name, 
                      DATA_PATH = DATA_PATH, 
                      GCS_FILE_PATH = GCS_FILE_PATH):    
    # Convert to json
    #https://gist.github.com/romgapuz/c7a4cedb85f090ac1b55383a58fa572c
    json_obj = json.loads(json.dumps(my_dict, default=str))
    
    # Save json locally
    json.dump(json_obj, open(f"{DATA_PATH}{name}.json", "w", encoding='utf-8'))
    
    # Put the json object in GCS. 
    fs.put(f"{DATA_PATH}{name}.json", f"{GCS_FILE_PATH}{name}.json")
    print(f"Saved {name}")
    
    
def open_request_json(name, DATA_PATH = DATA_PATH, 
                       GCS_FILE_PATH = GCS_FILE_PATH):
    # Download object from GCS bucket
    gcs_json = fs.get(f"{GCS_FILE_PATH}{name}.json", f"{DATA_PATH}{name}.json")
    my_dict = json.load(open(f"{DATA_PATH}{name}.json"))
    
    return my_dict