import pandas as pd
import geopandas as gpd
from update_vars import ANALYSIS_DATE, BORDER_BUFFER_METERS, GCS_PATH
from utils import read_census_tracts
from segment_speed_utils import helpers
import shapely

def attach_projected_stop_times(analysis_date: str):
    '''
    
    '''
    st_dir_cols = ['trip_instance_key', 'stop_sequence', 'stop_meters', 'stop_id']
    st_dir = helpers.import_scheduled_stop_times(analysis_date, columns=st_dir_cols, get_pandas=True,
                                                 with_direction=True)
    st = helpers.import_scheduled_stop_times(analysis_date, get_pandas=True)
    trips = helpers.import_scheduled_trips(analysis_date, columns=['trip_id', 'trip_instance_key', 'feed_key',
                                                                  'shape_array_key'])
    st = st.merge(trips, on = ['feed_key', 'trip_id'])
    return st.merge(st_dir, on = ['trip_instance_key', 'stop_sequence', 'stop_id'])

if __name__ == "__main__":
    
    shapes = helpers.import_scheduled_shapes(analysis_date, crs=geography_utils.CA_NAD83Albers_m)
    