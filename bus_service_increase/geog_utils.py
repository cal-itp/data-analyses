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
        print(itp_id)
        if alternate_df is None:
            shapes = (
                tbl.gtfs_schedule.shapes()
                >> filter(_.calitp_itp_id == int(itp_id))
                >> collect()
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
            unique_shapes = list(shapes.shape_id.unique())
            for route in unique_shapes:
                try:
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
                except: 
                    print(f"unable to grab: {itp_id: {route}}")

            all_routes = (
                all_routes.to_crs(CRS)
                .sort_values(["calitp_itp_id", "shape_id"])
                .drop_duplicates()
                .reset_index(drop=True)
            )

    return all_routes


