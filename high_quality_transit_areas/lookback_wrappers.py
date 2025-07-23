import pandas as pd
import geopandas as gpd

from segment_speed_utils import helpers
import datetime as dt
import yaml

def read_published_operators(current_date: str,
                             published_operators_yaml: str = "../gtfs_funnel/published_operators.yml",
                            lookback_days = 95):
    
        # Read in the published operators file
    with open(published_operators_yaml) as f:
        published_operators_dict = yaml.safe_load(f)
        
    currant_date = dt.date.fromisoformat(current_date)
    lookback_limit = currant_date - dt.timedelta(days=lookback_days)
    
    # Convert the published operators file into a dict mapping dates to an iterable of operators
    patch_operators_dict = {
        str(key):published_operators_dict[key] for
        key in published_operators_dict.keys()
        if key > lookback_limit and key < currant_date}
    
    return patch_operators_dict

def get_lookback_trips(published_operators_dict: dict, trips_cols: list) -> pd.DataFrame:
    '''
    Get trips according to published_operators_dict.
    Trips reflect the most recent date each operator appeared.
    '''
    lookback_trips = []
    for date in published_operators_dict.keys():
        lookback_trips += [helpers.import_scheduled_trips(date, filters=[['name', 'in', published_operators_dict[date]]],
                                  columns=trips_cols).assign(lookback_date = date)]
    return pd.concat(lookback_trips)

def lookback_trips_ix(lookback_trips: pd.DataFrame) -> pd.DataFrame:
    '''
    Keep identifier cols and drop duplicates, use for filtering in other lookback
    functions.
    '''
    lookback_trips_ix = lookback_trips[['name', 'feed_key', 'schedule_gtfs_dataset_key',
                                   'shape_array_key', 'lookback_date']].drop_duplicates()
    return lookback_trips_ix