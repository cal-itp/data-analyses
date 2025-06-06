import os
os.environ['USE_PYGEOS'] = '0'
os.environ["CALITP_BQ_MAX_BYTES"] = str(800_000_000_000)
from shared_utils import gtfs_utils_v2
from calitp_data_analysis import geography_utils

import pandas as pd
from siuba import *
import geopandas as gpd
import shapely

import conveyal_vars

regions = conveyal_vars.conveyal_regions
TARGET_DATE = conveyal_vars.TARGET_DATE

def create_region_gdf():
    # https://shapely.readthedocs.io/en/stable/reference/shapely.box.html#shapely.box
    # xmin, ymin, xmax, ymax
    to_bbox = lambda x: [x['west'], x['south'], x['east'], x['north']]
    df = pd.DataFrame(regions).transpose().reset_index().rename(columns={'index':'region'})
    df['bbox'] = df.apply(to_bbox, axis=1)
    df['geometry'] = df.apply(lambda x: shapely.geometry.box(*x.bbox), axis = 1)
    df = df >> select(-_.bbox)
    region_gdf = gpd.GeoDataFrame(df, crs=geography_utils.WGS84).to_crs(geography_utils.CA_NAD83Albers_m)
    return region_gdf

def get_stops_dates(feeds_on_target: pd.DataFrame, feed_key_column_name: str = "feed_key", date_column_name: str = "date"):
    """Get stops for the feeds in feeds_on_target based on their date"""
    all_stops = feeds_on_target.groupby(date_column_name)[feed_key_column_name].apply(
        lambda feed_key_column: gtfs_utils_v2.get_stops(
            selected_date=feed_key_column.name,
            operator_feeds=feed_key_column
        )
    )
    return all_stops

def join_stops_regions(region_gdf: gpd.GeoDataFrame, feeds_on_target: pd.DataFrame):
    #all_stops = gtfs_utils_v2.get_stops(selected_date=TARGET_DATE, operator_feeds=feeds_on_target.feed_key)
    all_stops = get_stops_dates(feeds_on_target).to_crs(geography_utils.CA_NAD83Albers_m)
    region_join = gpd.sjoin(region_gdf, all_stops)
    regions_and_feeds = region_join >> distinct(_.region, _.feed_key)
    return regions_and_feeds

if __name__ == '__main__':
    feeds_on_target = pd.read_parquet(f'{conveyal_vars.GCS_PATH}feeds_{TARGET_DATE}.parquet')
    region_gdf = create_region_gdf()
    regions_and_feeds = join_stops_regions(region_gdf, feeds_on_target)
    regions_and_feeds = regions_and_feeds >> inner_join(_, feeds_on_target >> select(_.feed_key, _.gtfs_dataset_name, _.base64_url,
                                                                                _.date), on = 'feed_key')
    regions_and_feeds.to_parquet(f'{conveyal_vars.GCS_PATH}regions_feeds_{TARGET_DATE}.parquet')