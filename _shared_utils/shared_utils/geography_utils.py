"""
Utility functions for geospatial data.
Some functions for dealing with census tract or other geographic unit dfs.
"""
from typing import Union

import dask.dataframe as dd
import geopandas as gpd
import pandas as pd
import shapely

WGS84 = "EPSG:4326"
CA_StatePlane = "EPSG:2229"  # units are in feet
CA_NAD83Albers = "EPSG:3310"  # units are in meters

SQ_MI_PER_SQ_M = 3.86 * 10**-7
FEET_PER_MI = 5_280
SQ_FT_PER_SQ_MI = 2.788 * 10**7


def aggregate_by_geography(
    df: Union[pd.DataFrame, gpd.GeoDataFrame],
    group_cols: list,
    sum_cols: list = [],
    mean_cols: list = [],
    count_cols: list = [],
    nunique_cols: list = [],
    rename_cols: bool = False,
) -> pd.DataFrame:
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

    def aggregate_and_merge(
        df: Union[pd.DataFrame, gpd.GeoDataFrame],
        final_df: pd.DataFrame,
        group_cols: list,
        agg_cols: list,
        aggregate_function: str,
    ):

        agg_df = df.pivot_table(
            index=group_cols, values=agg_cols, aggfunc=aggregate_function
        ).reset_index()

        # https://stackoverflow.com/questions/34049618/how-to-add-a-suffix-or-prefix-to-each-column-name
        # Why won't .add_prefix or .add_suffix work?
        if rename_cols:
            for c in agg_cols:
                agg_df = agg_df.rename(columns={c: f"{c}_{aggregate_function}"})

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


# Laurie's example: https://github.com/cal-itp/data-analyses/blob/752eb5639771cb2cd5f072f70a06effd232f5f22/gtfs_shapes_geo_examples/example_shapes_geo_handling.ipynb
# have to convert to linestring
def make_linestring(x: str) -> shapely.geometry.LineString:
    # shapely errors if the array contains only one point
    if len(x) > 1:
        # each point in the array is wkt
        # so convert them to shapely points via list comprehension
        as_wkt = [shapely.wkt.loads(i) for i in x]
        return shapely.geometry.LineString(as_wkt)


def make_routes_gdf(
    df: pd.DataFrame,
    crs: str = "EPSG:4326",
) -> gpd.GeoDataFrame:
    """
    Parameters:

    crs: str, a projected coordinate reference system.
        Defaults to EPSG:4326 (WGS84)
    """
    # Use apply to use map_partitions
    # https://stackoverflow.com/questions/70829491/dask-map-partitions-meta-when-using-lambda-function-to-add-column
    ddf = dd.from_pandas(df, npartitions=1)

    ddf["geometry"] = ddf.pt_array.apply(make_linestring, meta=("geometry", "geometry"))
    shapes = ddf.compute()

    # convert to geopandas; re-project if needed
    gdf = gpd.GeoDataFrame(
        shapes.drop(columns="pt_array"), geometry="geometry", crs=WGS84
    ).to_crs(crs)

    return gdf


def create_point_geometry(
    df: pd.DataFrame,
    longitude_col: str = "stop_lon",
    latitude_col: str = "stop_lat",
    crs: str = WGS84,
) -> gpd.GeoDataFrame:
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
    # Default CRS for stop_lon, stop_lat is WGS84
    df = df.assign(
        geometry=gpd.points_from_xy(df[longitude_col], df[latitude_col], crs=WGS84)
    )

    # ALlow projection to different CRS
    gdf = gpd.GeoDataFrame(df).to_crs(crs)

    return gdf


def create_segments(
    geometry: Union[
        shapely.geometry.linestring.LineString,
        shapely.geometry.multilinestring.MultiLineString,
    ],
    segment_distance: int,
) -> gpd.GeoSeries:
    """
    Splits a Shapely LineString into smaller LineStrings.
    If a MultiLineString passed, splits each LineString in that collection.

    Input a geometry column, such as gdf.geometry.

    Double check: segment_distance must be given in the same units as the CRS!
    """
    lines = []

    if hasattr(geometry, "geoms"):  # check if MultiLineString
        linestrings = geometry.geoms
    else:
        linestrings = [geometry]

    for linestring in linestrings:
        for i in range(0, int(linestring.length), segment_distance):
            lines.append(shapely.ops.substring(linestring, i, i + segment_distance))

    return lines


def cut_segments(
    gdf: gpd.GeoDataFrame,
    group_cols: list = ["calitp_itp_id", "calitp_url_number", "route_id"],
    segment_distance: int = 1_000,
) -> gpd.GeoDataFrame:
    """
    Cut segments from linestrings at defined segment lengths.
    Make sure segment distance is defined in the same CRS as the gdf.

    group_cols: list of columns.
                The set of columns that represents how segments should be cut.
                Ex: for transit route, it's calitp_itp_id-calitp_url_number-route_id
                Ex: for highways, it's Route-RouteType-County-District.

    Returns a gpd.GeoDataFrame where each linestring row is now multiple
    rows (each at the pre-defined segment_distance). A new column called
    `segment_sequence` is also created, which differentiates each
    new row created (since they share the same group_cols).
    """
    EPSG_CODE = gdf.crs.to_epsg()

    segmented = gpd.GeoDataFrame()

    gdf = gdf[group_cols + ["geometry"]].drop_duplicates().reset_index(drop=True)

    for row in gdf.itertuples():
        row_geom = getattr(row, "geometry")
        segment = create_segments(row_geom, int(segment_distance))

        to_append = pd.DataFrame()
        to_append["geometry"] = segment
        for c in group_cols:
            to_append[c] = getattr(row, c)

        segmented = pd.concat([segmented, to_append], axis=0, ignore_index=True)

        segmented = segmented.assign(
            temp_index=segmented.sort_values(group_cols).reset_index(drop=True).index
        )

    # Why would there be NaNs?
    # could this be coming from group_cols...one of the cols has a NaN in some rows?
    segmented = segmented[segmented.temp_index.notna()]

    segmented = (
        segmented.assign(
            segment_sequence=(
                segmented.groupby(group_cols)["temp_index"].transform("rank") - 1
            )
            .astype(int)
            .astype(str)
        )
        .sort_values(group_cols)
        .reset_index(drop=True)
        .drop(columns="temp_index")
    )

    segmented2 = gpd.GeoDataFrame(segmented, crs=f"EPSG:{EPSG_CODE}")

    return segmented2
