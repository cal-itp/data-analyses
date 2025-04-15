# import intake
import pandas as pd
import geopandas as gpd
from calitp_data_analysis import geography_utils


from update_vars import ANALYSIS_DATE, BORDER_BUFFER_METERS
from segment_speed_utils import helpers
from utils import read_census_tracts
import uuid

def intersection_hash(row):
    '''
    Get unique hash of intersection zones.
    No need to keep both t1 x t2 and t2 x t1
    '''
    t1 = int(row.tract_1[2:]) #  drop state code
    t2 = int(row.tract_2[2:])
    row_tracts = [t1, t2]
    row_tracts.sort() #  modifies inplace
    return hash(tuple(row_tracts))

def find_borders(tracts_gdf: gpd.GeoDataFrame,
                border_buffer: int = BORDER_BUFFER_METERS
) -> gpd.GeoDataFrame:
    '''
    '''
    tracts_gdf = tracts_gdf.copy()
    tracts_gdf.geometry = tracts_gdf.buffer(border_buffer)
    borders = gpd.overlay(tracts_gdf, tracts_gdf)
    borders = borders[borders['tract_1'] != borders['tract_2']]
    # for dropping mirrored borders
    borders['intersection_hash'] = borders.apply(intersection_hash, axis=1)
    borders = borders.drop_duplicates(subset=['intersection_hash'])
    # for more elegant tracking
    borders['intersection_id'] = [str(uuid.uuid4()) for _ in range(borders.shape[0])] 
    return borders

def find_shapes_in_tracts_borders(shape_stops, tracts, borders):
    
    '''
    Transit service intensity analysis segments are cut by shape,
    and are each census tract and/or border zone that shape passes
    through.

    We'll count
    '''
    shape_stops_tracts_borders = (pd.concat([tracts, borders])
                              .sjoin(shape_stops)
                              .drop(columns='index_right')
                             )



if __name__ == "__main__":
    
    tracts = read_census_tracts(cols=['Tract', 'geometry'])
    shapes = helpers.import_scheduled_shapes(ANALYSIS_DATE)
    borders = find_borders(tracts)
    st = helpers.import_scheduled_stop_times(analysis_date=ANALYSIS_DATE, columns=['feed_key', 'trip_id', 'stop_id'], get_pandas=True)
    trips = helpers.import_scheduled_trips(ANALYSIS_DATE, columns=['shape_array_key', 'trip_id', 'feed_key'])
    stops = helpers.import_scheduled_stops(ANALYSIS_DATE, columns=['feed_key', 'stop_id', 'geometry'])
    
    shape_stops = (stops.merge(st, on = ['feed_key', 'stop_id'])
     .merge(trips, on = ['feed_key', 'trip_id'])
     .drop_duplicates(subset=['feed_key', 'shape_array_key', 'stop_id'])
     .dropna()
    )

    