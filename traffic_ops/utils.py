"""
Utility functions from _shared_utils

Since Airflow runs in data-infra,
rather than install all the shared_utils, 
just lift the relevant functions needed for scripts.
"""
import fsspec
import geopandas as gpd
import os
import pandas as pd
import shapely

os.environ["CALITP_BQ_MAX_BYTES"] = str(50_000_000_000)

from calitp.tables import tbl
from calitp.storage import get_fs
from calitp import query_sql
from siuba import *

fs = get_fs()
WGS84 = "EPSG:4326"

#----------------------------------------------------------#
## _shared_utils/utils.py
# https://github.com/cal-itp/data-analyses/blob/main/_shared_utils/shared_utils/utils.py
#----------------------------------------------------------#
def geoparquet_gcs_export(gdf, GCS_FILE_PATH, FILE_NAME):
    '''
    Save geodataframe as parquet locally, 
    then move to GCS bucket and delete local file.
    
    gdf: geopandas.GeoDataFrame
    GCS_FILE_PATH: str. Ex: gs://calitp-analytics-data/data-analyses/my-folder/
    FILE_NAME: str. Filename.
    '''
    gdf.to_parquet(f"./{FILE_NAME}.parquet")
    fs.put(f"./{FILE_NAME}.parquet", f"{GCS_FILE_PATH}{FILE_NAME}.parquet")
    os.remove(f"./{FILE_NAME}.parquet")

    
def download_geoparquet(GCS_FILE_PATH, FILE_NAME, save_locally=False):
    """
    Parameters:
    GCS_FILE_PATH: str. Ex: gs://calitp-analytics-data/data-analyses/my-folder/
    FILE_NAME: str, name of file (without the .parquet).
                Ex: test_file (not test_file.parquet)
    save_locally: bool, defaults to False. if True, will save geoparquet locally.
    """
    object_path = fs.open(f"{GCS_FILE_PATH}{FILE_NAME}.parquet")
    gdf = gpd.read_parquet(object_path)
    
    if save_locally is True:
        gdf.to_parquet(f"./{FILE_NAME}.parquet")
    
    return gdf
    
    
#----------------------------------------------------------#
## _shared_utils/geography_utils.py
# https://github.com/cal-itp/data-analyses/blob/main/_shared_utils/shared_utils/geography_utils.py
#----------------------------------------------------------#
# Function to construct the SQL condition for make_routes_gdf()
def construct_condition(SELECTED_DATE, INCLUDE_ITP_LIST):
    def unpack_list_make_or_statement(INCLUDE_ITP_LIST):
        new_cond = ""

        for i in range(0, len(INCLUDE_ITP_LIST)):
            cond = f"calitp_itp_id = {INCLUDE_ITP_LIST[i]}"
            if i == 0:
                new_cond = cond
            else:
                new_cond = new_cond + " OR " + cond

        new_cond = "(" + new_cond + ")"

        return new_cond

    operator_or_statement = unpack_list_make_or_statement(INCLUDE_ITP_LIST)

    date_condition = (
        f'(calitp_extracted_at <= "{SELECTED_DATE}" AND '
        f'calitp_deleted_at > "{SELECTED_DATE}")'
    )

    condition = operator_or_statement + " AND " + date_condition

    return condition


# Run the sql query with the condition in long-form
def create_shapes_for_subset(SELECTED_DATE, ITP_ID_LIST):
    condition = construct_condition(SELECTED_DATE, ITP_ID_LIST)

    sql_statement = f"""
        SELECT
            calitp_itp_id,
            calitp_url_number,
            shape_id,
            pt_array

        FROM `views.gtfs_schedule_dim_shapes_geo`

        WHERE
            {condition}
        """

    df = query_sql(sql_statement)

    return df


def make_routes_gdf(SELECTED_DATE, CRS="EPSG:4326", ITP_ID_LIST=None):
    """
    Parameters:

    SELECTED_DATE: str or datetime
        Ex: '2022-1-1' or datetime.date(2022, 1, 1)
    CRS: str, a projected coordinate reference system.
        Defaults to EPSG:4326 (WGS84)
    ITP_ID_LIST: list or None
            Defaults to all ITP_IDs except ITP_ID==200.
            For a subset of operators, include a list, such as [182, 100].

    All operators for selected date: ~11 minutes
    """

    if ITP_ID_LIST is None:
        df = (
            tbl.views.gtfs_schedule_dim_shapes_geo()
            >> filter(
                _.calitp_extracted_at <= SELECTED_DATE,
                _.calitp_deleted_at > SELECTED_DATE,
            )
            >> filter(_.calitp_itp_id != 200)
            >> select(_.calitp_itp_id, _.calitp_url_number, _.shape_id, _.pt_array)
            >> collect()
        )
    else:
        df = create_shapes_for_subset(SELECTED_DATE, ITP_ID_LIST)

    # Laurie's example: https://github.com/cal-itp/data-analyses/blob/752eb5639771cb2cd5f072f70a06effd232f5f22/gtfs_shapes_geo_examples/example_shapes_geo_handling.ipynb
    # have to convert to linestring
    def make_linestring(x):

        # shapely errors if the array contains only one point
        if len(x) > 1:
            # each point in the array is wkt
            # so convert them to shapely points via list comprehension
            as_wkt = [shapely.wkt.loads(i) for i in x]
            return shapely.geometry.LineString(as_wkt)

    # apply the function
    df["geometry"] = df.pt_array.apply(make_linestring)

    # convert to geopandas; geometry column contains the linestring
    gdf = gpd.GeoDataFrame(df, geometry="geometry", crs=WGS84)

    # Project, if necessary
    gdf = gdf.to_crs(CRS)

    return gdf


# Function to deal with edge cases where operators do not submit the optional shapes.txt
# Use stops data / stop sequence to handle
def make_routes_line_geom_for_missing_shapes(df, CRS="EPSG:4326"):
    """
    Parameters:
    df: pandas.DataFrame.
        Compile a dataframe from gtfs_schedule_trips.
        Find the trips that aren't in `shapes.txt`.
        https://github.com/cal-itp/data-analyses/blob/main/traffic_ops/create_routes_data.py#L63-L69
        Use that dataframe here.

    CRS: str, a projected coordinated reference system.
            Defaults to EPSG:4326 (WGS84)
    """
    if "shape_id" not in df.columns:
        df = df.assign(shape_id=df.route_id)

    # Make a gdf
    gdf = gpd.GeoDataFrame(
        df,
        geometry=gpd.points_from_xy(df.stop_lon, df.stop_lat),
        crs=WGS84,
    )

    # Count the number of stops for a given shape_id
    # Must be > 1 (need at least 2 points to make a line)
    gdf = gdf.assign(
        num_stops=(gdf.groupby("shape_id")["stop_sequence"].transform("count"))
    )

    # Drop the shape_ids that can't make a line
    gdf = (
        gdf[gdf.num_stops > 1]
        .reset_index(drop=True)
        .assign(stop_sequence=gdf.stop_sequence.astype(int))
        .drop(columns="num_stops")
    )

    # shapely make linestring with groupby
    # https://gis.stackexchange.com/questions/366058/pandas-dataframe-to-shapely-linestring-using-groupby-sortby
    group_cols = ["calitp_itp_id", "calitp_url_number", "shape_id"]

    gdf2 = (
        gdf.sort_values(group_cols + ["stop_sequence"])
        .groupby(group_cols)["geometry"]
        .apply(lambda x: shapely.geometry.LineString(x.tolist()))
    )
    
    # Turn geoseries into gdf
    gdf2 = gpd.GeoDataFrame(gdf2, geometry="geometry", crs=WGS84).reset_index()
        
    gdf2 = (
        gdf2.to_crs(CRS)
        .sort_values(["calitp_itp_id", "calitp_url_number", "shape_id"])
        .drop_duplicates()
        .reset_index(drop=True)
    )

    return gdf2
