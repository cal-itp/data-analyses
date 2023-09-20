import os
from rt_analysis import rt_filter_map_plot
import pandas as pd
import geopandas as gpd
import datetime as dt
from siuba import *
from shared_utils.geography_utils import WGS84, CA_NAD83Albers
import shapely

import numpy as np
from calitp_data_analysis import get_fs
import glob
from tqdm.notebook import tqdm


def read_signal_excel(path):
    '''
    one-off excel format from Traffic Ops
    '''
    
    df = pd.read_excel(path)
    df.columns = df.columns.str.replace(' ', '_').str.lower()
    gdf = gpd.GeoDataFrame(df, geometry=gpd.points_from_xy(df.longitude, df.latitude), crs=WGS84)
    
    return gdf

def concatenate_speedmap_segments(progress_df: pd.DataFrame = None,
                             itp_id_list: list = None,
                             analysis_date: dt.datetime = None,
                             pbar: tqdm = None,
                             filter_args: dict = None):
    '''
    get polygon segments from legacy speedmap workflow, with relevant ids attached
    relatively fast if already ran for date, slow otherwise
    
    progress_df: see data_analyses/ca_transit_speed_maps
    filter_dict: dict of args to RtFilterMapper.set_filter
    '''
    
    df_present = isinstance(progress_df, pd.DataFrame)
    assert df_present or itp_id_list and analysis_date, 'must provide either a speedmap progress df or itp_ids and analysis date'
    assert (not (df_present and itp_id_list)
                and (not (df_present and analysis_date))), 'must provide either a speedmap progress df or itp_ids and analysis date'
    itp_id_list = itp_id_list or progress_df.organization_itp_id.to_list()
    analysis_date = analysis_date or progress_df.analysis_date.iloc[0]
    all_segment_gdfs = []
    for itp_id in itp_id_list:
        print(itp_id)
        try:
            rt_day = rt_filter_map_plot.from_gcs(itp_id, analysis_date, pbar)
            if filter_args:
                rt_day.set_filter(**filter_args)
            _m = rt_day.segment_speed_map(how='low_speeds', no_title=True, shn=True,
                                 no_render=True
                                )

            dmv_proj = rt_day.detailed_map_view.to_crs(CA_NAD83Albers)
            # re-add some identifiers since we won't have the instance handy
            # dmv_proj['feed_key'] = rt_day.rt_trips.feed_key.iloc[0]
            dmv_proj['gtfs_dataset_key'] = rt_day.rt_trips.gtfs_dataset_key.iloc[0]
            dmv_proj['organization_name'] = rt_day.organization_name
            dmv_proj['system_p50_median'] = dmv_proj.p50_mph.quantile(.5)

            all_segment_gdfs += [dmv_proj]
        except Exception as e:
            print(f'{itp_id}, {e}')
    
    all_segment_gdfs = pd.concat(all_segment_gdfs)
    return all_segment_gdfs

def copy_segment_speeds(progress_df: pd.DataFrame = None,
                        itp_id_list: list = None,
                        analysis_date: dt.datetime = None):
    '''
    get line segments from legacy speedmap workflow
    must generate segments via all_day_speedmap_segments first, or else there will
    be no cached files in gcs to grab+concat
    
    progress_df: see data_analyses/ca_transit_speed_maps
    '''
    
    
    df_present = isinstance(progress_df, pd.DataFrame)
    assert df_present or itp_id_list and analysis_date, 'must provide either a speedmap progress df or itp_ids and analysis date'
    assert (not (df_present and itp_id_list)
                and (not (df_present and analysis_date))), 'must provide either a speedmap progress df or itp_ids and analysis date'
    itp_id_list = itp_id_list or progress_df.organization_itp_id.to_list()
    analysis_date = analysis_date or progress_df.analysis_date.iloc[0]
    fs = get_fs()
    base = 'gs://calitp-analytics-data/data-analyses/rt_delay/v2_segment_speed_views/'
    pattern = f'_{analysis_date}_All_Day.parquet'
    patterns = [str(itp_id) + pattern for itp_id in itp_id_list]
    remote_paths = [base + pattern for pattern in patterns]
    local_path = f'./segment_lines_all_{analysis_date}'
    os.system(f'rm -rf {local_path}')
    os.system(f'mkdir {local_path}')
    fs.get(remote_paths, f'{local_path}/')
    
    # https://gis.stackexchange.com/questions/394391/reading-all-shapefiles-in-folder-using-geopandas-and-then-clipping-them-all-iter
    files = glob.iglob(f'{local_path}/*')
    gdfs = (gpd.read_parquet(file) for file in files) # generator
    segment_lines_all = pd.concat(gdfs)
    
    os.system(f'rm -rf {local_path}')
    
    return segment_lines_all

def sjoin_signals(signal_gdf: gpd.GeoDataFrame,
                  segments_gdf: gpd.GeoDataFrame,
                  segments_lines_gdf: gpd.GeoDataFrame):
    '''
    signal_gdf: one-off format from traffic ops. primarily a spatial process,
    so exclude freeway ramp meters (only relevant to traffic joining fwy,
    which usually isn't transit)
    segments_gdf: geometry is polygons (buffered)
    segments_lines_gdf: geometry is linestrings (need for later approaching calc)
    '''
    
    signals = (signal_gdf
                   >> filter(_.tms_unit_type != 'Freeway Ramp Meters')
                   >> select(_.imms_id, _.location, _.geometry)
               ).copy()
    signals_points = signals.to_crs(CA_NAD83Albers)
    signals_buffered = signals_points.copy()
    signals_buffered.geometry = signals_buffered.buffer(150)

    joined = gpd.sjoin(segments_gdf, signals_buffered) >> select(-_.index_right)

    points_for_join = signals_points >> select(_.imms_id, _.signal_pt_geom == _.geometry)
    joined_signal_points = joined >> inner_join(_, points_for_join, on ='imms_id')

    # add line geometries from stop_segment_speed_view
    seg_lines = (segments_lines_gdf
                >> select(_.line_geom == _.geometry, _.shape_id, _.stop_sequence, _.stop_id)
                >> distinct(_.line_geom, _.shape_id, _.stop_sequence, _.stop_id)
            )
    # ideally a more robust join in the future
    joined_seg_lines = joined_signal_points >> inner_join(_, seg_lines, on = ['shape_id', 'stop_sequence', 'stop_id'])
    return joined_seg_lines

@np.vectorize
def vector_start(linestring):
    return shapely.ops.substring(linestring, 0, 0)

@np.vectorize
def vector_end(linestring):
    return shapely.ops.substring(linestring, linestring.length, linestring.length)

@np.vectorize
def vector_distance(point1, point2):
    return point1.distance(point2)

def determine_approaching(joined_seg_lines_gdf: gpd.GeoDataFrame):
    
    '''
    using vectorized shapely functions,
    determine if segment is approaching a signal or departing a signal.
    '''
    
    start_array = vector_start(joined_seg_lines_gdf.line_geom)
    end_array = vector_end(joined_seg_lines_gdf.line_geom)
    start_distances = vector_distance(start_array, joined_seg_lines_gdf.signal_pt_geom)
    end_distances = vector_distance(end_array, joined_seg_lines_gdf.signal_pt_geom)
    approaching = start_distances > end_distances
    assert len(approaching) == len(joined_seg_lines_gdf)
    joined_seg_lines_gdf['approaching'] = approaching
    return joined_seg_lines_gdf

def calculate_speed_score(df):
    twenty_mph = 20
    decile_raw = ((df.p50_mph - df.system_p50_median) / df.system_p50_median) * 10
    decile_raw_20 = ((df.p50_mph - twenty_mph) / twenty_mph) * 10
    decile_min = np.minimum(decile_raw, decile_raw_20)
    decile_rounded = np.round(decile_min, 0).astype('int64')
    # positive values are comparatively higher speeds, mask to 0 to indicate low priority
    decile_masked = decile_rounded.mask(decile_rounded > 0, 0)
    decile_score = np.absolute(decile_masked)
    df['speed_score'] = decile_score
    return df

def calculate_variability_score(df):
    variability = df.fast_slow_ratio
    variability_low_mask = variability.mask(variability <= 1, 0)
    variability_scaled = np.round(variability * 3, 0)
    variability_high_mask = variability_scaled.mask(variability_scaled > 10, 10)
    variability_score = variability_high_mask.astype('int64')
    df['variability_score'] = variability_score
    return df

def calculate_frequency_score(df):
    frequency = df.trips_per_hour
    frequency_scaled = np.round(frequency * 2, 0)
    frequency_score = frequency_scaled.mask(frequency_scaled > 10, 10).astype('int64')
    df['frequency_score'] = frequency_score
    return df

def calculate_scores(joined_seg_lines_gdf):
    
    to_calculate = (joined_seg_lines_gdf
          >> filter(_.p50_mph < 20) # filter out fast segments
          >> filter(_.approaching)
          >> select(-_.signal_pt_geom, -_.line_geom)
      )
    
    to_calculate = calculate_speed_score(to_calculate)
    to_calculate = calculate_variability_score(to_calculate)
    to_calculate = calculate_frequency_score(to_calculate)
    to_calculate['transit_score'] = to_calculate.speed_score + to_calculate.variability_score + to_calculate.frequency_score
    
    median_by_signal = to_calculate >> group_by(_.imms_id, _.location) >> summarize(speed_score = _.speed_score.median(),
                                            variability_score = _.variability_score.median(),
                                            frequency_score = _.frequency_score.median(),
                                            overall_transit_score = _.transit_score.median(),
                                           )
    return median_by_signal >> arrange(-_.overall_transit_score)