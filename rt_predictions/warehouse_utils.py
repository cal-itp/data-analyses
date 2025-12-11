from typing import Literal

import gcsfs
import geopandas as gpd
import google.auth
import pandas as pd
import shapely
from calitp_data_analysis import geography_utils
from google.cloud import bigquery

fs = gcsfs.GCSFileSystem()
credentials, project = google.auth.default()


def convert_to_gdf(df: pd.DataFrame, geom_col: str, geom_type: Literal["point", "line"]) -> gpd.GeoDataFrame:
    """
    For stops, we want to make pt_geom a point.
    For vp_path and shapes, we want to make pt_array a linestring.
    """
    if geom_type == "point":
        df["geometry"] = [shapely.wkt.loads(x) for x in df[geom_col]]

    elif geom_type == "line":
        df["geometry"] = df[geom_col].apply(geography_utils.make_linestring)

    gdf = gpd.GeoDataFrame(df.drop(columns=geom_col), geometry="geometry", crs="EPSG:4326")

    return gdf


def basic_sql_query(project_name: str, dataset_name: str, table_name: str) -> str:
    """
    Set up the basic sql query needed, which is the entire table.
    """
    sql_query = f"SELECT * FROM  `{project_name}`.`{dataset_name}`.`{table_name}`"
    return sql_query


def add_sql_date_filter(date_col: str, start_date: str, end_date: str) -> str:
    """
    Add a where condition to filter by date, coerce the dates so sql_query is read correctly.
    """
    where_condition = f"WHERE {date_col} >= DATE('{start_date}') AND {date_col} <= DATE('{end_date}')"

    return where_condition


def download_table_no_date(
    sql_query: str,
):
    """
    Download table using google.cloud.bigquery.
    date_col is needed because most of our tables are partitioned by a date column,
    and these need to be coerced to datetime for parquet exports.
    """
    client = bigquery.Client()

    query_job = client.query(sql_query)

    df = query_job.result().to_dataframe()

    return df


def download_table_with_date(
    sql_query: str,
    date_col: str = "",
):
    """
    Download table using google.cloud.bigquery.
    date_col is needed because most of our tables are partitioned by a date column,
    and these need to be coerced to datetime for parquet exports.
    """
    df = download_table_no_date(sql_query).astype({date_col: "datetime64[ns]"})

    return df


def download_table_with_date_geom(
    sql_query: str,
    date_col: str = "",
    geom_col: str = "",
    geom_type: Literal["point", "line"] = "",
):
    """
    Download table using google.cloud.bigquery with geom, save out as gdf.
    Using the google.cloud.bigquery parses arrays as arrays, not strings.
    """
    df = download_table_with_date(
        sql_query,
        date_col,
    ).pipe(convert_to_gdf, geom_col, geom_type)

    return df
