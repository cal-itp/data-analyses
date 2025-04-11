"""
Grab all the operators by service date from
saved scheduled_trips tables from GCS.

Create a yaml that tells us the most recent
date available for each operator (schedule_gtfs_dataset_name).
"""
import pandas as pd
import pyaml # use pyaml because it gets us prettier indents than yaml

from pathlib import Path
from typing import Union

from shared_utils import gtfs_utils_v2, rt_dates
from segment_speed_utils import time_series_utils
import datetime

def filter_to_recent_date(df: pd.DataFrame) -> pd.DataFrame:
    """
    By schedule_gtfs_dataset_name, keep the most recent
    service_date that shows up in scheduled trips.
    """
    df2 = (df.groupby("name", group_keys=False)
           .service_date
           .max()
           .reset_index()
           .sort_values(["service_date", "name"], ascending=[False, True])
           .reset_index(drop=True)
           .astype({"service_date": "str"})
          )
    return df2


def export_results_yml(
    df: pd.DataFrame, 
    export_yaml: Union[str, Path]
):
    """
    Save out our results from df.
    Convert df into a dictionary and save out dictionary results as yaml.
    """
    # TODO: check this list manually and there will be some 
    # operator names that have more recent names that we are keeping,
    # so we can remove these from our yaml
    exclude_me = [
        "Flex",
    ]
        
    df2 = df.copy()
    
    for exclude_word in exclude_me:
    
        df2 = df2[~df2.name.str.contains(exclude_word)]
    
    # yaml export can have date as string
    # but yaml safe_load will automatically parse as datetime again
    my_dict = {
        **{
            date_key: df2[df2.service_date==date_key].name.tolist() 
            for date_key in df2.service_date.unique()
          }  
    }
    
    
    # sort_keys=False to prevent alphabetical sort (earliest date first)
    # because we want to main our results and yaml with most recent date first
    output = pyaml.dump(my_dict, sort_keys=False)
    
    with open(export_yaml, "w") as f:
        f.write(output)
    
    print(f"{export_yaml} exported")
    
    return

    
if __name__ == "__main__":
    
    from update_vars import (GTFS_DATA_DICT, 
                             COMPILED_CACHED_VIEWS, 
                             PUBLISHED_OPERATORS_YAML)
    
    TABLE = GTFS_DATA_DICT.schedule_downloads.trips

    public_feeds = gtfs_utils_v2.filter_to_public_schedule_gtfs_dataset_keys()

    operators = time_series_utils.concatenate_datasets_across_dates(
        COMPILED_CACHED_VIEWS,
        TABLE,
        rt_dates.y2025_dates + rt_dates.y2024_dates + rt_dates.y2023_dates,
        data_type = "df",
        get_pandas = True,
        filters = [[("gtfs_dataset_key", "in", public_feeds)]],
        columns = ["name"]
    ).drop_duplicates().pipe(filter_to_recent_date)
    
    current_year = str(datetime.datetime.now().year)
    assert (operators.service_date.str.contains(current_year)).any(), 'must add current calendar year, see README'

    export_results_yml(operators, PUBLISHED_OPERATORS_YAML)