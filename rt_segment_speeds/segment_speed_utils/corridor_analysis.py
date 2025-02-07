import pandas as pd
import geopandas as gpd
import numpy as np
from shared_utils import rt_utils, catalog_utils

from calitp_data_analysis import get_fs, geography_utils
from segment_speed_utils import helpers, time_series_utils, gtfs_schedule_wrangling
from segment_speed_utils.project_vars import SCHED_GCS, SEGMENT_GCS, GTFS_DATA_DICT, analysis_date

catalog = catalog_utils.get_catalog('gtfs_analytics_data')
CORRIDOR_BUFFER = 70 #  meters
CORRIDOR_RELEVANCE = 1000 #  meters, or half corridor length

def import_speedmap_segment_speeds(analysis_date: str) -> gpd.GeoDataFrame:
    
    path = f'{catalog.speedmap_segments.dir}{catalog.speedmap_segments.shape_stop_single_segment_detail}_{analysis_date}.parquet'
    detail = gpd.read_parquet(path)
    return detail

def get_max_frequencies(segment_speeds: gpd.GeoDataFrame) -> pd.DataFrame:
    
    frequencies = segment_speeds[['route_id', 'schedule_gtfs_dataset_key', 'trips_hr_sch']].drop_duplicates()
    frequencies = frequencies.groupby(['route_id', 'schedule_gtfs_dataset_key']).max().reset_index().sort_values('trips_hr_sch', ascending=False)
    return frequencies

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
    and measure its distance in meters, then buffer.
    '''
    
    shape_filtered = speed_segments_gdf.query("organization_source_record_id == @organization_source_record_id & shape_id == @shape_id")
    assert not shape_filtered.empty, 'empty shape, check shape_id'
    assert start_seg_id in shape_filtered.segment_id.values, 'start seg not in shape'
    assert end_seg_id in shape_filtered.segment_id.values, 'end seg not in shape'
    
    if start_seg_id == end_seg_id:
        filter_ids = [start_seg_id]
    else:
        shape_filtered = shape_filtered.assign(start_point = shape_filtered.geometry.apply(lambda x: x.boundary.geoms[0]),
                          end_point = shape_filtered.geometry.apply(lambda x: x.boundary.geoms[1])
                         )
        filter_ids = [start_seg_id, end_seg_id]
        current_seg_id = start_seg_id
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
    # print(corridor_start, corridor_end)
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
    
    Avoid capturing shapes merely crossing corridor by ensuring they run within the corridor for either
    half of the corridor length or the CORRIDOR_RELEVANCE threshold
    '''
    speed_segments_gdf = speed_segments_gdf.to_crs(geography_utils.CA_NAD83Albers_m)
    corridor_segments = speed_segments_gdf.clip(corridor_gdf)
    attach_geom = (corridor_segments.groupby(
        ['shape_array_key', 'segment_id','geometry']).agg(
        {'trips_hr_sch': 'max'})).reset_index()
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
    # corridor_relevance_threshold = min(half_corr, CORRIDOR_RELEVANCE)
    corridor_relevance_threshold = half_corr
    trip_speeds_df = trip_speeds_df.query('shape_length >= @corridor_relevance_threshold')
    trip_speeds_gdf = gpd.GeoDataFrame(trip_speeds_df, crs=geography_utils.CA_NAD83Albers_m)
    
    return trip_speeds_gdf

def validate_corridor_routes(corridor_gdf: gpd.GeoDataFrame, corridor_trips_gdf: gpd.GeoDataFrame):
    '''
    display table and map of shape directions with route short names for all routes
    identified to be in corridor
    '''
    validation_cols = ['route_short_name', 'shape_length', 'shape_array_key']
    m = corridor_gdf.explore(color='gray')
    display(corridor_trips_gdf.drop_duplicates(subset=validation_cols)[validation_cols])
    return corridor_trips_gdf[validation_cols + ['geometry']].explore(m=m, column='shape_length')

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
    trip_mph_floor: int = None,
    trip_percent_speedup: float = None
):
    '''
    Calculates potential transit priority effects using a floor speed and/or percent speed increase, or by number of seconds saved
    per-trip alone.
    
    If floor speed and percent speedup are both provided, use floor speed if median corridor speed < floor speed * (1-trip_percent_speedup).
    Otherwise use percent speedup.
    
    floor speed: assume all trips below floor speed in corridor achieve floor speed
    percent speedup: assume all trips run x percent faster
    seconds saved: assume all trips proceed through corridor/hotspot in x fewer seconds
    '''
    assert bool(trip_seconds_saved) ^ bool(trip_mph_floor or trip_percent_speedup), 'specify only trip_seconds_saved, or one/both of trip_mph_floor and trip_percent_speedup' #  ^ is XOR operator
    df = corridor_analysis_df
    print(f'median: {df.corridor_speed_mph.round(2).median()}mph... ', end='')
    if trip_percent_speedup and trip_percent_speedup > 1: trip_percent_speedup = trip_percent_speedup / 100
    if trip_mph_floor and trip_percent_speedup and df.corridor_speed_mph.median() >= (trip_mph_floor * (1-trip_percent_speedup) ):
        trip_mph_floor = None #  if median exceeding floor, use percent
    if trip_seconds_saved:
        df = df.assign(improved_corridor_seconds = (df['corridor_seconds'] - trip_seconds_saved).clip(lower=0),
                      intervention_assumption = f'each trip saves {trip_seconds_saved} seconds')
        df = df.assign(improved_corridor_speed_mps = df['corridor_meters'] / df['improved_corridor_seconds'])
        df = df.assign(improved_corridor_speed_mph = df['improved_corridor_speed_mps'] * rt_utils.MPH_PER_MPS)
    elif trip_percent_speedup and not trip_mph_floor: #  either percent alone specified or median speeds exceed floor
        print(f'percent mode: {trip_percent_speedup}')
        df = df.assign(improved_corridor_speed_mph = df['corridor_speed_mph'] * (1 + trip_percent_speedup),
                      intervention_assumption = f'{trip_percent_speedup*100}% trip speed increase')
        df = df.assign(improved_corridor_speed_mps = df['improved_corridor_speed_mph'] / rt_utils.MPH_PER_MPS)
        df = df.assign(improved_corridor_seconds = df['corridor_meters'] / df['improved_corridor_speed_mps'])
    elif trip_mph_floor:
        print(f'mph floor mode: {trip_mph_floor}mph')
        df = df.assign(improved_corridor_speed_mph = df['corridor_speed_mph'].clip(lower=trip_mph_floor),
                      intervention_assumption = f'trips achieve {trip_mph_floor}mph or existing spd if higher')
        df = df.assign(improved_corridor_speed_mps = df['improved_corridor_speed_mph'] / rt_utils.MPH_PER_MPS)
        df = df.assign(improved_corridor_seconds = df['corridor_meters'] / df['improved_corridor_speed_mps'])
    
    return df

def summarize_corridor_improvements(
    analysis_df: pd.DataFrame,
    frequencies: pd.DataFrame,
    extra_group_cols: list = []) -> pd.DataFrame:
    '''
    aggregate results of specified corridor improvements from analyze_corridor_improvements
    '''
    group_cols=['corridor_id', 'schedule_gtfs_dataset_key', 'intervention_assumption'] + extra_group_cols
    sum_cols = ['corridor_seconds', 'improved_corridor_seconds', 'delay_seconds',
                   'delay_minutes']
    array_cols = ['route_short_name', 'route_id']
    analysis_df = analysis_df.assign(delay_seconds = analysis_df.corridor_seconds - analysis_df.improved_corridor_seconds)
                  # corridor_miles = analysis_df.corridor_meters / rt_utils.METERS_PER_MILE)
    analysis_df = analysis_df.assign(delay_minutes = analysis_df.delay_seconds / 60)
    
    group = analysis_df.groupby(group_cols)

    analysis_df = group.agg({**{x:'sum' for x in sum_cols},
                    **{x:'unique' for x in array_cols},
                            'corridor_speed_mph': np.median})
    analysis_df = analysis_df.rename(columns={'corridor_speed_mph': 'median_corridor_mph'})
    analysis_df = analysis_df.merge(group.agg({'corridor_speed_mph':'count'}).rename(
        columns={'corridor_speed_mph':'n_trips_daily'}), on=group_cols)
    #  join in max route frequencies
    freq = (analysis_df.explode(['route_short_name', 'route_id']).reset_index(
                ).merge(frequencies, on=['route_id', 'schedule_gtfs_dataset_key'])
           )
    #  add frequencies to output to match array cols; allow inspection before summing delay metrics and frequencies
    analysis_df = (analysis_df.reset_index().merge(
        freq.groupby('schedule_gtfs_dataset_key').agg(
            {'trips_hr_sch': lambda x: list(x)}), on='schedule_gtfs_dataset_key')
         )
    return analysis_df.round(1)

def combine_corridor_operators(corridor_gdf):
    '''
    aggregate all transit operators in each corridor
    '''
    group_cols = ['corridor_id', 'corridor_name', 'geometry',
                 'intervention_assumption']
    overall = corridor_gdf.groupby(group_cols).agg({
        'corridor_miles': 'min', 'delay_minutes': 'sum', 'minutes_per_mile': 'sum', 'median_corridor_mph': np.median,
        'trips_per_hr_peak_directional': 'sum', 'n_trips_daily':'sum',
    }).reset_index()
    overall = overall.assign(average_trip_delay = overall.delay_minutes/overall.n_trips_daily)
    return overall.sort_values('minutes_per_mile', ascending=False)

def corridor_from_sheet(
    corridor_specifications: pd.DataFrame,
    speed_segments_gdf: gpd.GeoDataFrame,
    trip_speeds_df: pd.DataFrame,
    frequencies: pd.DataFrame,
    intervention_dict: dict,
    fwy_xpwy_floor: int = None):
    '''
    We've specified corridors in a spreadsheet. After reading that in, use this
    to iterate and analyze each corridor.
    '''
    all_corridors = []
    for _, row in corridor_specifications.iterrows():
        try:
            print(row["SHS Segment"])
            corr = corridor_from_segments(speed_segments_gdf=speed_segments_gdf, organization_source_record_id=row.organization_source_record_id, shape_id=row.shape_id,
                          start_seg_id=row.start_segment_id, end_seg_id=row.end_segment_id, name=row['SHS Segment'])
            corridor_trips = find_corridor_data(speed_segments_gdf, corr, trip_speeds_df)
            display(validate_corridor_routes(corr, corridor_trips))
            corridor_results = analyze_corridor_trips(corridor_trips)
            if hasattr(row, 'fwy_xpwy')  and row.fwy_xpwy:
                analyzed_interventions = intervention_dict.copy()
                analyzed_interventions['trip_mph_floor'] = fwy_xpwy_floor
                df = analyze_corridor_improvements(corridor_results, **analyzed_interventions)
            else:
                df = analyze_corridor_improvements(corridor_results, **intervention_dict)
            summ = summarize_corridor_improvements(df, frequencies).reset_index(drop=True)
            corr = pd.merge(corr, summ, on='corridor_id')
            corr = corr.assign(corridor_miles = corr.corridor_distance_meters / rt_utils.METERS_PER_MILE) #  from corridor def, not trip distance
            corr = corr.assign(minutes_per_mile = corr.delay_minutes / corr.corridor_miles)
            all_corridors += [corr]
        except Exception as e:
            print(f'failed for{row["SHS Segment"]}')
            print(e)
            pass
    all_corridors = pd.concat(all_corridors)
    all_corridors = all_corridors.assign(trips_per_hr_peak_directional = all_corridors.trips_hr_sch.map(lambda x: sum(x)))
    all_corridors = combine_corridor_operators(all_corridors)
    return gpd.GeoDataFrame(all_corridors, crs=geography_utils.CA_NAD83Albers_m)