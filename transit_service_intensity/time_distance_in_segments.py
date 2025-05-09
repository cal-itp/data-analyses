import pandas as pd
import geopandas as gpd
from update_vars import ANALYSIS_DATE, BORDER_BUFFER_METERS, GCS_PATH
from utils import read_census_tracts
from segment_speed_utils import helpers
from calitp_data_analysis import geography_utils
from shared_utils import rt_utils
import shapely

from dask.diagnostics import ProgressBar
ProgressBar().register()

import dask.dataframe as dd
import dask_geopandas as dg

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

def read_tsi_segs(analysis_date, shapes):
    tsi_segs = gpd.read_parquet(f'tsi_segments_{analysis_date}.parquet')
    tsi_segs = tsi_segs.drop(columns=['geometry'])

    shape_merged = (shapes.merge(tsi_segs, on='shape_array_key')
                         .rename(columns={'geometry': 'shape_geometry'}))

    shape_merged = shape_merged.assign(
        start_meters = shape_merged.shape_geometry.project(shape_merged.start)
    )
    shape_merged = shape_merged.sort_values('start_meters').reset_index(drop=True)
    cols = ['shape_array_key', 'tsi_segment_id', 'start_meters', 'tsi_segment_meters']
    shape_merged = shape_merged[cols]
    return shape_merged

def tract_border_time_by_trip(tracts_borders_trip_df: pd.DataFrame, st_proj_df: pd.DataFrame):
    '''
    '''
    
    one_trip = st_proj_df.query('trip_instance_key == @tracts_borders_trip_df.trip_instance_key.iloc[0]').sort_values('stop_sequence')
    shape_array = one_trip.stop_meters.to_numpy()
    dt_float_array = one_trip.arrival_sec.to_numpy()
    tracts_borders_trip_df['arrival_sec'] = tracts_borders_trip_df.start_meters.apply(
        rt_utils.time_at_position_numba, shape_array=shape_array, dt_float_array = dt_float_array)
    tracts_borders_trip_df = tracts_borders_trip_df.assign(arrival_sec_next = tracts_borders_trip_df.arrival_sec.shift(-1),
                                                   trip_instance_key = one_trip.trip_instance_key.iloc[0])
    tracts_borders_trip_df.loc[tracts_borders_trip_df.index.min(),'arrival_sec'] = one_trip.arrival_sec.min()
    tracts_borders_trip_df.loc[tracts_borders_trip_df.index.max(),'arrival_sec_next'] = one_trip.arrival_sec.max()
    tracts_borders_trip_df = tracts_borders_trip_df.assign(segment_seconds = tracts_borders_trip_df.arrival_sec_next - tracts_borders_trip_df.arrival_sec)
    
    return tracts_borders_trip_df

def dask_calculate_batch(
    tracts_borders_trip_df: pd.DataFrame,
    st_proj_df: pd.DataFrame,
    meta: pd.DataFrame) -> pd.DataFrame:
    
    ddf = dd.from_pandas(tracts_borders_trip_df, npartitions=100)
    ddf = (ddf.groupby('trip_instance_key', group_keys=False)
       .apply(tract_border_time_by_trip, st_proj_df = st_proj, meta=meta))
    df = ddf.compute()
    return df

if __name__ == "__main__":
    
    print(f'time_distance_in_segments {ANALYSIS_DATE}')
    shapes = helpers.import_scheduled_shapes(ANALYSIS_DATE, crs=geography_utils.CA_NAD83Albers_m)
    st_proj = attach_projected_stop_times(ANALYSIS_DATE)
    
    shape_merged = read_tsi_segs(ANALYSIS_DATE, shapes)
    tsi_segments_trips = shape_merged.merge(st_proj[['shape_array_key', 'trip_instance_key']].drop_duplicates(), on='shape_array_key')
    
    many_trip_test =(tsi_segments_trips.head(10)
                .groupby('trip_instance_key', group_keys=False)
                .apply(tract_border_time_by_trip, st_proj_df = st_proj))
    meta = many_trip_test[:0]
    dask_calculate_batch(tsi_segments_trips,
                         st_proj, meta).to_parquet(f'trip_tables_all_{ANALYSIS_DATE}.parquet')
    
    # trips = tsi_segments_trips.trip_instance_key.unique()
    # trips1 = trips[:len(trips)//10]
    # dask_calculate_batch(tsi_segments_trips.query('trip_instance_key.isin(@trips1)'),
    #                      st_proj, meta).to_parquet(f'trip_tables_batch1_{ANALYSIS_DATE}.parquet')
    # dask_calculate_batch(tsi_segments_trips.query('not trip_instance_key.isin(@trips1)'),
    #                      st_proj, meta).to_parquet(f'trip_tables_batch2_{ANALYSIS_DATE}.parquet')