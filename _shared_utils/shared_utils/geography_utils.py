"""
Utility functions for geospatial data.
Some functions for dealing with census tract or other geographic unit dfs.
"""
import os

import geopandas as gpd
import pandas as pd
import shapely
from calitp.tables import tbl
from siuba import *

os.environ["CALITP_BQ_MAX_BYTES"] = str(50_000_000_000)

WGS84 = "EPSG:4326"
CA_StatePlane = "EPSG:2229"  # units are in feet
CA_NAD83Albers = "EPSG:3310"  # units are in meters

SQ_MI_PER_SQ_M = 3.86 * 10 ** -7
FEET_PER_MI = 5_280
SQ_FT_PER_SQ_MI = 2.788 * 10 ** 7


def aggregate_by_geography(
    df,
    group_cols,
    sum_cols=[],
    mean_cols=[],
    count_cols=[],
    nunique_cols=[],
    rename_cols=False,
):
    """
    df: pandas.DataFrame or geopandas.GeoDataFrame.,
        The df on which the aggregating is done.
        If it's a geodataframe, it must exclude the tract's geometry column

    group_cols: list.
        List of columns to do the groupby, but exclude geometry.
    sum_cols: list.
        List of columns to calculate a sum with the groupby.
    mean_cols: list.
        List of columns to calculate an average with the groupby
        (beware: may want weighted averages and not simple average!!).
    count_cols: list.
        List of columns to calculate a count with the groupby.
    nunique_cols: list.
        List of columns to calculate the number of unique values with the groupby.
    rename_cols: boolean.
        Defaults to False. If True, will rename columns in sum_cols to have suffix `_sum`,
        rename columns in mean_cols to have suffix `_mean`, etc.

    Returns a pandas.DataFrame or geopandas.GeoDataFrame (same as input).
    """
    final_df = df[group_cols].drop_duplicates().reset_index()

    def aggregate_and_merge(df, final_df, group_cols, agg_cols, AGGREGATE_FUNCTION):

        agg_df = df.pivot_table(
            index=group_cols, values=agg_cols, aggfunc=AGGREGATE_FUNCTION
        ).reset_index()

        if rename_cols is True:
            # https://stackoverflow.com/questions/34049618/how-to-add-a-suffix-or-prefix-to-each-column-name
            # Why won't .add_prefix or .add_suffix work?
            for c in agg_cols:
                agg_df = agg_df.rename(columns={c: f"{c}_{AGGREGATE_FUNCTION}"})

        final_df = pd.merge(final_df, agg_df, on=group_cols, how="left", validate="1:1")
        return final_df

    if len(sum_cols) > 0:
        final_df = aggregate_and_merge(df, final_df, group_cols, sum_cols, "sum")

    if len(mean_cols) > 0:
        final_df = aggregate_and_merge(df, final_df, group_cols, mean_cols, "mean")

    if len(count_cols) > 0:
        final_df = aggregate_and_merge(df, final_df, group_cols, count_cols, "count")

    if len(nunique_cols) > 0:
        final_df = aggregate_and_merge(
            df, final_df, group_cols, nunique_cols, "nunique"
        )

    return final_df.drop(columns="index")


def attach_geometry(df, geometry_df, merge_col=["Tract"], join="left"):
    """
    df: pandas.DataFrame
        The df that needs tract geometry added.
    geometry_df: geopandas.GeoDataFrame
        The gdf that supplies the geometry.
    merge_col: list.
        List of columns to do the merge on.
    join: str.
        Specify whether it's a left, inner, or outer join.

    Returns a geopandas.GeoDataFrame
    """
    gdf = pd.merge(
        geometry_df.to_crs(WGS84),
        df,
        on=merge_col,
        how=join,
    )

    return gdf


# Function to take transit stop point data and create lines
def make_routes_shapefile(ITP_ID_LIST=[], CRS="EPSG:4326", alternate_df=None):
    """
    Parameters:
    ITP_ID_LIST: list. List of ITP IDs found in agencies.yml
    CRS: str. Default is WGS84, but able to re-project to another CRS.

    Returns a geopandas.GeoDataFrame, where each line is the operator-route-line geometry.
    """

    all_routes = gpd.GeoDataFrame()

    for itp_id in ITP_ID_LIST:
        if alternate_df is None:
            shapes = (
                tbl.gtfs_schedule.shapes()
                >> filter(_.calitp_itp_id == int(itp_id))
                >> collect()
            )

        elif alternate_df is not None:
            shapes = alternate_df.copy()

            # With historical gtfs_schedule_type2.shapes table, there is a valid shape_id
            if shapes.shape_id.isnull().sum() == 0:
                pass
            else:
                # shape_id is None, which will throw up an error later on when there's groupby
                # So, create shape_id and fill it in with route_id info
                shapes = shapes.assign(
                    shape_id=shapes.route_id,
                )

        # Make a gdf
        shapes = gpd.GeoDataFrame(
            shapes,
            geometry=gpd.points_from_xy(shapes.shape_pt_lon, shapes.shape_pt_lat),
            crs=WGS84,
        )

        # Count the number of stops for a given shape_id
        # Must be > 1 (need at least 2 points to make a line)
        shapes = shapes.assign(
            num_stops=(
                shapes.groupby("shape_id")["shape_pt_sequence"].transform("count")
            )
        )

        # Drop the shape_ids that can't make a line
        shapes = shapes[shapes.num_stops > 1].reset_index(drop=True)

        # Now, combine all the stops by stop sequence, and create linestring
        for route in shapes.shape_id.unique():
            single_shape = (
                shapes
                >> filter(_.shape_id == route)
                >> mutate(shape_pt_sequence=_.shape_pt_sequence.astype(int))
                # arrange in the order of stop sequence
                >> arrange(_.shape_pt_sequence)
            )

            # Convert from a bunch of points to a line (for a route, there are multiple points)
            route_line = shapely.geometry.LineString(list(single_shape["geometry"]))
            single_route = single_shape[
                ["calitp_itp_id", "calitp_url_number", "shape_id"]
            ].iloc[
                [0]
            ]  # preserve info cols
            single_route["geometry"] = route_line
            single_route = gpd.GeoDataFrame(single_route, crs=WGS84)

            # https://stackoverflow.com/questions/15819050/pandas-dataframe-concat-vs-append/48168086
            all_routes = pd.concat(
                [all_routes, single_route], ignore_index=True, axis=0
            )

    all_routes = (
        all_routes.to_crs(CRS)
        .sort_values(["calitp_itp_id", "shape_id"])
        .drop_duplicates()
        .reset_index(drop=True)
    )

    return all_routes


def create_point_geometry(
    df, longitude_col="stop_lon", latitude_col="stop_lat", crs=WGS84
):
    """
    Parameters:
    df: pandas.DataFrame to turn into geopandas.GeoDataFrame,
        default dataframe in mind is gtfs_schedule.stops

    longitude_col: str, column name corresponding to longitude
                    in gtfs_schedule.stops, this column is "stop_lon"

    latitude_col: str, column name corresponding to latitude
                    in gtfs_schedule.stops, this column is "stop_lat"

    crs: str, coordinate reference system for point geometry
    """
    df = df.assign(
        geometry=gpd.points_from_xy(df[longitude_col], df[latitude_col], crs=crs)
    )

    gdf = gpd.GeoDataFrame(df)
    return gdf
