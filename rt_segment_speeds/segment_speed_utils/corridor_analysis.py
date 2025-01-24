import pandas as pd
import geopandas as gpd
from shared_utils import rt_utils, catalog_utils

from calitp_data_analysis import get_fs, geography_utils
from segment_speed_utils import helpers, time_series_utils, gtfs_schedule_wrangling
from segment_speed_utils.project_vars import SCHED_GCS, SEGMENT_GCS, GTFS_DATA_DICT, analysis_date

catalog = catalog_utils.get_catalog('gtfs_analytics_data')
CORRIDOR_BUFFER = 70 #  70 meters
CORRIDOR_RELEVANCE = 500 #  500 meters, or half corridor length

def describe_cleaning(df:pd.DataFrame, cleaning_query:str, message:str):
    '''
    print percentage of rows in a df that match a query
    cleaning_query: passed to pd.DataFrame.query
    message: printed message
    
    currently does not return anything or actually filter df
    '''
    to_clean = df.query(cleaning_query)
    pct = round((to_clean.shape[0] / df.shape[0]) * 100, 1)
    print(f"{pct} percent of {message}")
    
def import_trip_speeds(analysis_date: str):
    path = f'{catalog.speedmap_segments.dir}{catalog.speedmap_segments.stage4}_{analysis_date}.parquet'
    st4 = pd.read_parquet(path)
    describe_cleaning(st4, 'speed_mph.isna()', 'segments have no speed')
    st4 = st4[~st4['speed_mph'].isna()]
    return st4

def corridor_from_segments(
    speed_segments_gdf: gpd.GeoDataFrame,
    organization_source_record_id: str,
    shape_id: str,
    start_seg_id: str,
    end_seg_id: str,
    name: str = None
) -> gpd.GeoDataFrame:
    '''
    Replicates legacy RtFilterMapper.autocorridor functionality.
    Optionally used to specify a corridor using a shape_id and two segment_id,
    for example in reference to published speedmaps.
    
    Can also specify a corridor using external GIS tools as a linestring, project
    and measure its distance in meters, then buffer by 100m.
    '''
    
    shape_filtered = speed_segments_gdf.query("organization_source_record_id == @organization_source_record_id & shape_id == @shape_id")
    
    shape_filtered = shape_filtered.assign(start_point = shape_filtered.geometry.apply(lambda x: x.boundary.geoms[0]),
                      end_point = shape_filtered.geometry.apply(lambda x: x.boundary.geoms[1])
                     )

    filter_ids = [start_seg_id, end_seg_id]
    current_seg_id = start_seg_id
    assert not shape_filtered.empty, 'empty shape, check shape_id'
    assert start_seg_id in shape_filtered.segment_id.values, 'start seg not in shape'
    assert end_seg_id in shape_filtered.segment_id.values, 'end seg not in shape'

    for _ in shape_filtered.segment_id:
        if current_seg_id == end_seg_id: break
        current_end = shape_filtered.loc[shape_filtered['segment_id'] == current_seg_id]['end_point'].iloc[0]
        next_segment = shape_filtered.loc[shape_filtered['start_point'] == current_end]
        assert not next_segment.empty, f'unable to locate segment after {current_seg_id}'
        current_seg_id = next_segment.segment_id.iloc[0]
        filter_ids += next_segment.segment_id.to_list()
        
    relevant_segments = shape_filtered.query("segment_id in @filter_ids").drop_duplicates(subset='segment_id')
    corridor = relevant_segments.dissolve()[['schedule_gtfs_dataset_key', 'shape_array_key', 'shape_id',
                                            'name', 'organization_source_record_id', 'geometry']]
    corridor_start = corridor.geometry.iloc[0].boundary.geoms[0]
    corridor_end = corridor.geometry.iloc[0].boundary.geoms[1]
    print(corridor_start, corridor_end)
    corridor = corridor.to_crs(geography_utils.CA_NAD83Albers_m).assign(corridor_distance_meters = lambda x: x.geometry.length)
    corridor.geometry = corridor.buffer(CORRIDOR_BUFFER) #  70m corridor buffer
    corridor = corridor.assign(corridor_id = hash(organization_source_record_id+shape_id+start_seg_id+end_seg_id),
                              corridor_name = name)
    return corridor

def find_corridor_data(
    speed_segments_gdf: gpd.GeoDataFrame,
    corridor_gdf: gpd.GeoDataFrame,
    trip_speeds_df: pd.DataFrame
) -> gpd.GeoDataFrame:
    '''
    With a buffered corridor defined, use the aggregated speed segments data to find relevant segments,
    then merge with trip-level speeds.
    '''
    speed_segments_gdf = speed_segments_gdf.to_crs(geography_utils.CA_NAD83Albers_m)
    corridor_segments = speed_segments_gdf.clip(corridor_gdf)
    attach_geom = corridor_segments[['shape_array_key', 'segment_id', 'trips_hr_sch',
                                     'geometry']].drop_duplicates()
    trip_speeds_df = attach_geom.merge(trip_speeds_df, on=['shape_array_key', 'segment_id']).assign(
                        corridor_id = corridor_gdf.corridor_id.iloc[0])
    
    trip_speeds_df['shape_length'] = trip_speeds_df.geometry.apply(lambda x: x.length)
    shape_lengths = (trip_speeds_df.drop_duplicates(
        subset=['schedule_gtfs_dataset_key', 'segment_id', 'shape_id']).groupby(
        ['shape_id', 'schedule_gtfs_dataset_key'])[['shape_length']].sum(
        ).reset_index()
                    )
    trip_speeds_df = trip_speeds_df.drop(columns=['shape_length']).merge(shape_lengths, on=['shape_id', 'schedule_gtfs_dataset_key'])
    half_corr = corridor_gdf.corridor_distance_meters.iloc[0] / 2
    corridor_relevance_threshold = min(half_corr, CORRIDOR_RELEVANCE)
    trip_speeds_df = trip_speeds_df.query('shape_length >= @corridor_relevance_threshold')
    
    return trip_speeds_df

def analyze_corridor_trips(
    corridor_trips_gdf: gpd.GeoDataFrame
):
    '''
    Calculate trip-level statistics for corridor.
    
    Currently applies some data cleaning to exclude trips with zero seconds in corridor
    and trips above 80mph
    '''
    grouped = corridor_trips_gdf.groupby(['trip_instance_key'])
    min_stops = grouped[['stop_meters', 'arrival_time_sec']].min().add_suffix('_min')
    max_stops = grouped[['subseq_stop_meters', 'subseq_arrival_time_sec']].max().add_suffix('_max')

    df = min_stops.join(max_stops)
    df = df.assign(
    corridor_meters = df['subseq_stop_meters_max'] - df['stop_meters_min'],
    corridor_seconds = df['subseq_arrival_time_sec_max'] - df['arrival_time_sec_min']
        )
    df = df.assign(corridor_speed_mps = df['corridor_meters'] / df['corridor_seconds'])
    df = df.assign(corridor_speed_mph = df['corridor_speed_mps'] * rt_utils.MPH_PER_MPS)
    
    describe_cleaning(df, 'corridor_seconds <= 0', 'trips with zero seconds')
    describe_cleaning(df, 'corridor_speed_mph > 80', 'trips with speeds > 80mph')
    
    corridor_trips_usable = df.query('corridor_seconds > 0 & corridor_speed_mph <= 80')
    trip_identifiers = corridor_trips_gdf[['trip_instance_key', 'route_short_name', 'route_id',
                   'shape_array_key', 'shape_id', 'schedule_gtfs_dataset_key',
                    'time_of_day', 'corridor_id']].drop_duplicates()

    corridor_trips_identified = (corridor_trips_usable.reset_index()
                                 .merge(trip_identifiers, on='trip_instance_key')
                                 .drop(columns=['stop_meters_min', 'arrival_time_sec_min', 'subseq_stop_meters_max',
                                               'subseq_arrival_time_sec_max']
                                      )
                                )
    
    return corridor_trips_identified

def analyze_corridor_improvements(
    corridor_analysis_df: pd.DataFrame,
    trip_seconds_saved: int = None,
    trip_mph_target: int = None
):
    '''
    Translate time savings into speed increase, or vice-versa
    '''
    assert (trip_seconds_saved or trip_mph_target) and not (trip_seconds_saved and trip_mph_target), 'specify exactly one'
    df = corridor_analysis_df
    if trip_seconds_saved:
        df = df.assign(improved_corridor_seconds = (df['corridor_seconds'] - trip_seconds_saved).clip(lower=0))
        df = df.assign(improved_corridor_speed_mps = df['corridor_meters'] / df['improved_corridor_seconds'])
        df = df.assign(improved_corridor_speed_mph = df['improved_corridor_speed_mps'] * rt_utils.MPH_PER_MPS)
    elif trip_mph_target:
        df = df.assign(improved_corridor_speed_mph = df['corridor_speed_mph'].clip(lower=trip_mph_target))
        df = df.assign(improved_corridor_speed_mps = df['improved_corridor_speed_mph'] / rt_utils.MPH_PER_MPS)
        df = df.assign(improved_corridor_seconds = df['corridor_meters'] / df['improved_corridor_speed_mps'])
    
    return df