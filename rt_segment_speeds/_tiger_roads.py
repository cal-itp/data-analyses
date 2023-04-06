import geopandas as gpd
import pandas as pd
from calitp_data_analysis.sql import to_snakecase
import datetime


from segment_speed_utils.project_vars import analysis_date
from segment_speed_utils import helpers
from shared_utils import  dask_utils, geography_utils, utils

GCS_FILE_PATH = "gs://calitp-analytics-data/data-analyses/"
SHARED_GCS = f"{GCS_FILE_PATH}shared_data/"

def gtfs_shapes_operators(date):
    """
    Load and merge gtfs_shapes 
    with trips to get operator and 
    feed key information.
    
    Returns a gpd.DataFrame.
    """
    gtfs_shapes = helpers.import_scheduled_shapes(date).compute()
    
    trips = helpers.import_scheduled_trips(date,(),['name','shape_array_key']).compute().drop_duplicates()
    
    m1 = pd.merge(gtfs_shapes, trips, how="outer", on="shape_array_key")
    return m1

def order_operators(date):
    """
    Re order a list of operators 
    so some of the largest ones will be at the top of 
    the list.
    """
    operator_list = helpers.import_scheduled_trips(analysis_date,(),['name']).compute().sort_values('name')
    operator_list = operator_list.name.unique().tolist()
    
    # Reorder list so the biggest operators are at the beginning
    # based on NTD services data 
    big_operators = ['LA DOT Schedule',
     'LA Metro Bus Schedule',
     'LA Metro Rail Schedule',
     'Bay Area 511 Muni Schedule',
     'Bay Area 511 AC Transit Schedule',
     'Bay Area 511 Santa Clara Transit Schedule',
     'Bay Area 511 BART Schedule',
     'San Diego Schedule','OCTA Schedule','Sacramento Schedule',
    ]
    i = 0
    for operator in big_operators:
        operator_list.remove(operator)
        operator_list.insert(i, operator)
        ++i
    return operator_list