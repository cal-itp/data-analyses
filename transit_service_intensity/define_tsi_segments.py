import pandas as pd
import geopandas as gpd
from update_vars import ANALYSIS_DATE, BORDER_BUFFER_METERS, GCS_PATH
from utils import read_census_tracts
from segment_speed_utils import helpers
import shapely

from tqdm import tqdm
tqdm.pandas(desc=f"TSI Segments Progress {ANALYSIS_DATE}")

def overlay_to_borders(
    shape_gdf: gpd.GeoDataFrame,
    border_gdf: gpd.GeoDataFrame,
    sensitivity_dist: int = BORDER_BUFFER_METERS * 4
                 ):
    '''
    
    '''
    overlaid = shape_gdf.overlay(border_gdf, how='intersection')
    overlaid = overlaid.query('geometry.length > @sensitivity_dist')
    return overlaid

def overlay_to_tracts(
    shape_gdf_no_border: gpd.GeoDataFrame,
    tract_gdf: gpd.GeoDataFrame,
                 ):
    '''
    
    '''
    tract_gdf = tract_gdf[['tract', 'geometry']]
    return shape_gdf_no_border.overlay(tract_gdf, how='intersection')

def overlay_tracts_borders(
    shape_gdf: gpd.GeoDataFrame,
    tract_gdf: gpd.GeoDataFrame,
    border_gdf: gpd.GeoDataFrame,
    sensitivity_dist: int = BORDER_BUFFER_METERS * 4
):
    '''
    '''
    border_gdf = border_gdf.drop(columns=['intersection_hash'])
    try:
        border_overlaid = overlay_to_borders(shape_gdf, border_gdf, sensitivity_dist)
        not_border = shape_gdf.overlay(border_overlaid, how='difference')
        tract_overlaid = overlay_to_tracts(not_border, tracts)
        tracts_and_borders = (pd.concat([tract_overlaid, border_overlaid])
                              .explode(index_parts=False)
                              .reset_index(drop=True)
                              .query('geometry.length > @sensitivity_dist')
                             )
        tracts_and_borders = tracts_and_borders.assign(
            border = ~tracts_and_borders.tract_2.isna(),
            start = tracts_and_borders.geometry.apply(lambda x: shapely.Point(x.coords[0])),
            # end = tracts_and_borders.geometry.apply(lambda x: shapely.Point(x.coords[-1])),
            tsi_segment_id = tracts_and_borders.tract.combine_first(tracts_and_borders.intersection_id).astype(str),
            tsi_segment_meters = tracts_and_borders.geometry.length
        )
        return tracts_and_borders
    except Exception as e:
        print(f'{shape_gdf}, {e}')
        
if __name__ == "__main__":
    
    print(f'define_tsi_segments {ANALYSIS_DATE}')
    tracts = read_census_tracts(ANALYSIS_DATE)
    shapes = helpers.import_scheduled_shapes(ANALYSIS_DATE)
    borders = gpd.read_parquet(f'borders_{ANALYSIS_DATE}.parquet')
    
    trip_cols = ['gtfs_dataset_key', 'name', 'trip_id',
        'shape_id', 'shape_array_key', 'route_id',
        'route_key', 'direction_id', 'route_short_name',
        'trip_instance_key', 'feed_key']

    trips = (helpers.import_scheduled_trips(ANALYSIS_DATE, columns=trip_cols)
        .dropna(subset=['shape_id'])
        )
    
    tsi_segs = (shapes
           .groupby('shape_array_key')
           .progress_apply(overlay_tracts_borders, tract_gdf=tracts, border_gdf=borders)
           .reset_index(drop=True)
          )
    tsi_segs.to_parquet(f'tsi_segments_{ANALYSIS_DATE}.parquet')
    