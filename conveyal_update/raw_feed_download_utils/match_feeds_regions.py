import os

import geopandas as gpd
import pandas as pd
import shapely
from calitp_data_analysis import geography_utils
from shared_utils import gtfs_utils_v2

from .entities import BoundingBoxDict

os.environ["USE_PYGEOS"] = "0"
os.environ["CALITP_BQ_MAX_BYTES"] = str(800_000_000_000)


def create_region_gdf(regions: BoundingBoxDict) -> gpd.GeoDataFrame:
    # https://shapely.readthedocs.io/en/stable/reference/shapely.box.html#shapely.box
    # xmin, ymin, xmax, ymax
    def to_bbox(region_series: pd.Series) -> list:
        return [region_series["west"], region_series["south"], region_series["east"], region_series["north"]]

    df = pd.DataFrame(regions).transpose().reset_index().rename(columns={"index": "region"})
    df["bbox"] = df.apply(to_bbox, axis=1)
    df["geometry"] = df.apply(lambda x: shapely.geometry.box(*x.bbox), axis=1)
    df = df.drop("bbox", axis=1)
    region_gdf = gpd.GeoDataFrame(df, crs=geography_utils.WGS84).to_crs(geography_utils.CA_NAD83Albers_m)
    return region_gdf


def get_stops_dates(
    feeds_on_target: pd.DataFrame, feed_key_column_name: str = "feed_key", date_column_name: str = "date"
):  # check type
    """Get stops for the feeds in feeds_on_target based on their date"""
    print("SAVING feeds_on_target")
    feeds_on_target.to_csv("FEEDS_ON_TARGET.csv")
    all_stops = feeds_on_target.groupby(date_column_name)[feed_key_column_name].apply(
        lambda feed_key_column: gtfs_utils_v2.get_stops(
            selected_date=feed_key_column.name, operator_feeds=feed_key_column
        )
    )
    return all_stops


def join_stops_regions(
    region_gdf: gpd.GeoDataFrame, feeds_on_target: pd.DataFrame, region_name=None
) -> gpd.GeoDataFrame:
    all_stops = get_stops_dates(feeds_on_target).to_crs(geography_utils.CA_NAD83Albers_m)
    region_join = gpd.sjoin(region_gdf, all_stops)
    # Want to keep one row per feed and region
    # Region may not be present
    feed_key_columns = ["feed_key"]
    if region_name is not None:
        feed_key_columns.append(region_name)
    regions_and_feeds = region_join[feed_key_columns].drop_duplicates()
    return regions_and_feeds
