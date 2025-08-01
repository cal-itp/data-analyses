import geopandas as gpd
import pandas as pd
import numpy as np
from segment_speed_utils import helpers, gtfs_schedule_wrangling
from update_vars import (analysis_date, EXPORT_PATH, GCS_FILE_PATH, PROJECT_CRS,
SEGMENT_BUFFER_METERS, MS_TRANSIT_THRESHOLD, SHARED_STOP_THRESHOLD,
TARGET_AREA_DIFFERENCE, BRANCHING_OVERLAY_BUFFER)
import create_aggregate_stop_frequencies
import lookback_wrappers

from tqdm import tqdm
tqdm.pandas()
# !pip install calitp-data-analysis==2025.6.24
from calitp_data_analysis.gcs_geopandas import GCSGeoPandas
gcsgp = GCSGeoPandas()

def get_explode_singles(
    single_route_aggregation: pd.DataFrame,
    ms_precursor_threshold: int | float
) -> pd.DataFrame:
    """
    Find all stops with single-route frequencies above the major stop precursor threshold.
    """
    single_qual = (single_route_aggregation.query('am_max_trips_hr >= @ms_precursor_threshold & pm_max_trips_hr >= @ms_precursor_threshold')
                   .explode('route_dir')
                   .sort_values(['schedule_gtfs_dataset_key','stop_id', 'route_dir'])[['schedule_gtfs_dataset_key','stop_id', 'route_dir']]
                  )
    return single_qual

def get_trips_with_route_dir(analysis_date: str, published_operators_dict: dict) -> pd.DataFrame:
    '''
    pass in published_operators_dict to enable lookback, also return lookback index
    for shapes query
    '''
    trips_cols = ["feed_key", "gtfs_dataset_key", "trip_id",
               "route_id", "direction_id", "route_type",
              "shape_array_key", "route_short_name", "name"]
    trips = helpers.import_scheduled_trips(
    analysis_date,
    columns = trips_cols,
    get_pandas = True
    )
    lookback_trips = lookback_wrappers.get_lookback_trips(published_operators_dict, trips_cols)
    lookback_trips_ix = lookback_wrappers.lookback_trips_ix(lookback_trips)
    trips = pd.concat([trips, lookback_trips])
    trips = trips[trips['route_type'].isin(['3', '11'])] #  bus only
    trips.direction_id = trips.direction_id.fillna(0).astype(int).astype(str)
    trips['route_dir'] = trips[['route_id', 'direction_id']].agg('_'.join, axis=1)
    
    return trips, lookback_trips_ix

def get_shapes_with_lookback(analysis_date: str, published_operators_dict: dict, lookback_trips_ix: pd.DataFrame):
    '''
    Get shapes (including lookback) and add route_dir and area.
    '''
    outside_amtrak_shapes = gtfs_schedule_wrangling.amtrak_trips(
    analysis_date = analysis_date, inside_ca = False).shape_array_key.unique()
    shapes = gtfs_schedule_wrangling.longest_shape_by_route_direction(
        analysis_date = analysis_date
    ).query(
        'shape_array_key not in @outside_amtrak_shapes'
    ).fillna({"direction_id": 0}).astype({"direction_id": "int"})
    lookback_hqta_shapes = lookback_wrappers.get_lookback_hqta_shapes(published_operators_dict, lookback_trips_ix, no_drop=True)
    shapes = pd.concat([shapes, lookback_hqta_shapes])
    shapes = shapes.assign(route_dir = shapes.apply(lambda x: str(x.route_id) + '_' + str(x.direction_id), axis=1))
    shapes.geometry = shapes.buffer(BRANCHING_OVERLAY_BUFFER)
    shapes = shapes.assign(area = shapes.geometry.map(lambda x: x.area))
    return shapes
    
def evaluate_overlaps(gtfs_dataset_key: str, qualify_dict: dict, shapes: gpd.GeoDataFrame, show_map: bool = False) -> list:
    """
    For each route_dir determined to be partially collinear with another, check symmetric difference
    to evaluate if each route can take riders from the shared trunk to unique destinations not served
    by the other route ("X" or "Y" branching). Symmetric distance spatial threshold is derived from
    update_vars.TARGET_METERS_DIFFERENCE, 5km as of July 2025.
    """
    this_feed_qual = {key.split(gtfs_dataset_key)[1][2:]:qualify_dict[key] for key in qualify_dict.keys() if key.split('__')[0] == gtfs_dataset_key}
    qualify_pairs = [tuple(key.split('__')) for key in this_feed_qual.keys()]

    qualify_sets = [set(x) for x in qualify_pairs]
    qualify_sets = set(map(frozenset, qualify_sets))

    unique_qualify_pairs_possible = [list(x) for x in qualify_sets]

    unique_qualify_pairs = []
    for pair in unique_qualify_pairs_possible:
        print(f'{pair}...', end='')
        these_shapes = shapes.query('route_dir.isin(@pair) & schedule_gtfs_dataset_key == @gtfs_dataset_key')
        first_row = these_shapes.iloc[0:1][['schedule_gtfs_dataset_key', 'route_dir', 'shape_array_key', 'geometry']]
        sym_diff = first_row.overlay(these_shapes.iloc[1:2][['route_dir', 'geometry']], how='symmetric_difference')
        sym_diff = sym_diff.assign(area = sym_diff.geometry.map(lambda x: x.area),
                              route_dir = sym_diff.route_dir_1.fillna(sym_diff.route_dir_2))
        diff_area = sym_diff.area.sum()
        area_ratios = (sym_diff.area / TARGET_AREA_DIFFERENCE)
        if (sym_diff.area > TARGET_AREA_DIFFERENCE).all():
            print(f'passed, {area_ratios[0]:.2f} and {area_ratios[1]:.2f} times area target')
            m = these_shapes.explore(color='gray', tiles='CartoDB Positron')
            if show_map: display(sym_diff.explore(column='route_dir', m=m, tiles='CartoDB Positron'))
            unique_qualify_pairs += [pair]
        else:
            print(f'failed, {area_ratios[0]:.2f} and {area_ratios[1]:.2f} times area target')
            if show_map: display(these_shapes.explore(column='route_dir', tiles='CartoDB Positron'))
            
    return unique_qualify_pairs

def find_stops_this_pair(feed_stops: pd.DataFrame, one_feed_pair: list) -> pd.DataFrame:
    feed_stops = (feed_stops.explode(column='route_dir')
                  .query('route_dir in @one_feed_pair')
                  .groupby(['schedule_gtfs_dataset_key', 'stop_id'])[['route_dir']]
                  .count()
                  .reset_index()
                 )
    return feed_stops.query('route_dir > 1').drop(columns=['route_dir'])

def find_stops_this_feed(gtfs_dataset_key: str,
                         max_arrivals_by_stop_single: pd.DataFrame,
                         unique_qualify_pairs: list) -> pd.DataFrame:
    """
    Get all stops in shared trunk section for a route_dir pair. These are major transit stops.
    """
    feed_stops = max_arrivals_by_stop_single.query('schedule_gtfs_dataset_key == @gtfs_dataset_key')
    stop_dfs = []
    for pair in unique_qualify_pairs:
        these_stops = find_stops_this_pair(feed_stops, pair)
        stop_dfs += [these_stops]    
    if len(stop_dfs) > 0:
        feed_add = pd.concat(stop_dfs)
        return feed_add

def match_spatial_format(branching_stops_gdf: gpd.GeoDataFrame) -> gpd.GeoDataFrame:
    """
    Conform to existing pipeline format.
    """
    gdf = branching_stops_gdf.rename(columns={'schedule_gtfs_dataset_key': 'schedule_gtfs_dataset_key_primary'})
    gdf = gdf.assign(schedule_gtfs_dataset_key_secondary = gdf.schedule_gtfs_dataset_key_primary,
                    hqta_type = 'major_stop_bus')
    return gdf

if __name__ == '__main__':
    
    published_operators_dict = lookback_wrappers.read_published_operators(analysis_date)
    trips, lookback_trips_ix = get_trips_with_route_dir(analysis_date, published_operators_dict)
                               
    shapes = helpers.import_scheduled_shapes(analysis_date, columns=['shape_array_key', 'geometry'])
    
    shapes = get_shapes_with_lookback(analysis_date, published_operators_dict, lookback_trips_ix)
    
    max_arrivals_by_stop_single = pd.read_parquet(f"{GCS_FILE_PATH}max_arrivals_by_stop_single_route.parquet")
    singles_explode = get_explode_singles(max_arrivals_by_stop_single, MS_TRANSIT_THRESHOLD).explode('route_dir')
    
    share_counts = {}
    (singles_explode.groupby(['schedule_gtfs_dataset_key', 'stop_id'])
     .progress_apply(create_aggregate_stop_frequencies.accumulate_share_count, share_counts=share_counts))
    
    qualify_dict = {key: share_counts[key] for key in share_counts.keys() if share_counts[key] >= SHARED_STOP_THRESHOLD}
    feeds_to_filter = np.unique([key.split('__')[0] for key in qualify_dict.keys()])
    
    hcd_branching_stops = []
    for gtfs_dataset_key in feeds_to_filter:
        unique_qualify_pairs = evaluate_overlaps(gtfs_dataset_key, show_map=False, shapes=shapes, qualify_dict=qualify_dict)
        this_feed_stops = find_stops_this_feed(gtfs_dataset_key, max_arrivals_by_stop_single, unique_qualify_pairs)
        hcd_branching_stops += [this_feed_stops]
    hcd_branching_stops = pd.concat(hcd_branching_stops).pipe(match_spatial_format)
    gcsgp.geo_data_frame_to_parquet(hcd_branching_stops, f"{GCS_FILE_PATH}branching_major_stops.parquet")
    
    