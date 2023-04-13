"""
GTFS utils for v1.
Remaining functions are used for supporting v1 speedmaps
backwards compatibility.
"""
import datetime
from typing import Union

import geopandas as gpd
import pandas as pd
from shared_utils import geography_utils, gtfs_utils_v2
from siuba import *

GCS_PROJECT = "cal-itp-data-infra"


# ----------------------------------------------------------------#
# v1 speedmaps dependencies
# ----------------------------------------------------------------#
def get_route_shapes(
    selected_date: Union[str, datetime.date],
    itp_id_list: list[int] = None,
    get_df: bool = True,
    crs: str = geography_utils.WGS84,
    trip_df: pd.DataFrame = None,
    custom_filtering: dict = None,
) -> gpd.GeoDataFrame:
    """
    Return a subset of geography_utils.make_routes_gdf()
    to only have the `shape_id` values present on a selected day.

    geography_utils.make_routes_gdf() only selects based on calitp_extracted_at
    and calitp_deleted_at date range.

    Allow a pre-existing trips table to be supplied.
    If not, run a fresh trips query.

    Custom_filtering doesn't filter in the query (which relies on trips query),
    but can filter out after it's a gpd.GeoDataFrame
    """
    if not trip_df:
        raise NameError("input an existing trip dataframe from GCS")

    # When a pre-existing table is given, convert it to pd.DataFrame
    # even if a LazyTbl is given
    elif trip_df:
        route_shapes = (
            make_routes_gdf(selected_date=selected_date, crs=crs, itp_id_list=itp_id_list)
            .drop(columns=["pt_array"])
            .drop_duplicates()
            .reset_index(drop=True)
        )

        shape_id_cols = ["calitp_itp_id", "calitp_url_number", "shape_id"]

        route_shapes_on_day = pd.merge(
            route_shapes,
            trip_df[shape_id_cols].drop_duplicates(),
            on=shape_id_cols,
            how="inner",
        ).drop_duplicates()

        route_shapes_on_day = route_shapes_on_day >> gtfs_utils_v2.filter_custom_col(custom_filtering)

        return route_shapes_on_day


def make_routes_gdf(
    selected_date: Union[str, datetime.datetime],
    crs: str = geography_utils.WGS84,
    itp_id_list: list = None,
):
    """
    Parameters:
    selected_date: str or datetime
        Ex: '2022-1-1' or datetime.date(2022, 1, 1)
    crs: str, a projected coordinate reference system.
        Defaults to EPSG:4326 (WGS84)
    itp_id_list: list or None
            Defaults to all ITP_IDs except ITP_ID==200.
            For a subset of operators, include a list, such as [182, 100].
    All operators for selected date: ~11 minutes
    """

    if itp_id_list is None:
        df = (
            tbls.views.gtfs_schedule_dim_shapes_geo()
            >> filter(
                _.calitp_extracted_at <= selected_date,
                _.calitp_deleted_at > selected_date,
            )
            >> filter(_.calitp_itp_id != 200)
            >> select(_.calitp_itp_id, _.calitp_url_number, _.shape_id, _.pt_array)
            >> collect()
        )
    else:
        df = create_shapes_for_subset(selected_date, itp_id_list)

    # apply the function
    df["geometry"] = df.pt_array.apply(geography_utils.make_linestring)

    # convert to geopandas; geometry column contains the linestring, re-project if needed
    gdf = gpd.GeoDataFrame(df, geometry="geometry", crs=geography_utils.WGS84).to_crs(crs)

    return gdf
