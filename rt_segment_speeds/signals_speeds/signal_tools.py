import os
from rt_analysis import rt_filter_map_plot
import pandas as pd
import geopandas as gpd
import datetime as dt
from calitp_data_analysis.geography_utils import WGS84, CA_NAD83Albers_m
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

            dmv_proj = rt_day.detailed_map_view.to_crs(CA_NAD83Albers_m)
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
                        analysis_date: dt.datetime = None,
                        date_exceptions: dict = None
                       ):
    '''
    get line segments from legacy speedmap workflow
    must generate segments via all_day_speedmap_segments first, or else there will
    be no cached files in gcs to grab+concat
    
    progress_df: see data_analyses/ca_transit_speed_maps
    date_exceptions: itp_id:dt.datetime
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
    patterns = [str(itp_id) + pattern for itp_id in itp_id_list if itp_id not in date_exceptions.keys()]
    exception_patterns = [str(itp_id) + f'_{date_exceptions[itp_id]}_All_Day.parquet' for itp_id in date_exceptions.keys()]
    patterns += exception_patterns
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
    
    signals = signal_gdf.loc[
        signal_gdf["tms_unit_type"] != "Freeway Ramp Meters",
        ["imms_id", "objectid", "location", signal_gdf.geometry.name]
    ].copy()
    
    signals_points = signals.to_crs(CA_NAD83Albers_m)
    signals_buffered = signals_points.copy()
    signals_buffered.geometry = signals_buffered.buffer(150)

    joined = gpd.sjoin(segments_gdf, signals_buffered).drop("index_right", axis=1)

    points_for_join = signals_points.rename_geometry("signal_pt_geom")[["signal_pt_geom", "imms_id", "objectid"]]
    joined_signal_points = joined.merge(points_for_join, on="objectid", how="inner", validate="many_to_one")

    # add line geometries from stop_segment_speed_view
    seg_lines = (segments_lines_gdf
        .rename_geometry("line_geom")[
            ["line_geom", "shape_id", "segment_id", "organization_source_record_id"]
        ].drop_duplicates(keep="first")
    )
    # ideally a more robust join in the future
    joined_seg_lines = joined_signal_points.merge(
        seg_lines,
        how="inner",
        on=["shape_id", "segment_id"],
    )
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

def determine_approaching(joined_seg_lines_gdf: gpd.GeoDataFrame) -> pd.Series:
    
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
    return approaching
    joined_seg_lines_gdf['approaching'] = approaching # lets not mess with mutability
    #return pd.concat([joined_seg_lines_gdf, pd.Series(approaching, name="approaching")], axis=1)
    
# Archivable
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