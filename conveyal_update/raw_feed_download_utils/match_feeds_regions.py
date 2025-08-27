import os
os.environ['USE_PYGEOS'] = '0'
os.environ["CALITP_BQ_MAX_BYTES"] = str(800_000_000_000)
from shared_utils import gtfs_utils_v2
from calitp_data_analysis import geography_utils
from .entities import BoundingBoxDict

import pandas as pd
import geopandas as gpd
import shapely

def create_region_gdf(regions: BoundingBoxDict) -> gpd.GeoDataFrame:
    # https://shapely.readthedocs.io/en/stable/reference/shapely.box.html#shapely.box
    # xmin, ymin, xmax, ymax
    to_bbox = lambda x: [x['west'], x['south'], x['east'], x['north']]
    df = pd.DataFrame(regions).transpose().reset_index().rename(columns={'index':'region'})
    df['bbox'] = df.apply(to_bbox, axis=1)
    df['geometry'] = df.apply(lambda x: shapely.geometry.box(*x.bbox), axis = 1)
    df = df.drop("bbox", axis=1)
    region_gdf = gpd.GeoDataFrame(df, crs=geography_utils.WGS84).to_crs(geography_utils.CA_NAD83Albers_m)
    return region_gdf

def get_stops_dates(feeds_on_target: pd.DataFrame, feed_key_column_name: str = "feed_key", date_column_name: str = "date"): # check type
    """Get stops for the feeds in feeds_on_target based on their date"""
    all_stops = feeds_on_target.groupby(date_column_name)[feed_key_column_name].apply(
        lambda feed_key_column: gtfs_utils_v2.get_stops(
            selected_date=feed_key_column.name,
            operator_feeds=feed_key_column
        )
    )
    return all_stops

def join_stops_regions(region_gdf: gpd.GeoDataFrame, feeds_on_target: pd.DataFrame) -> gpd.GeoDataFrame: 
    all_stops = get_stops_dates(feeds_on_target).to_crs(geography_utils.CA_NAD83Albers_m)
    region_join = gpd.sjoin(region_gdf, all_stops)
    regions_and_feeds = region_join[["region", "feed_key"]].drop_duplicates()
    return regions_and_feeds
