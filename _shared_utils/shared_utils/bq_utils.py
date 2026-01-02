"""
pandas_gbq utils to download Big Query tables
"""

from typing import Literal

import geopandas as gpd
import google.auth
import pandas as pd
import pandas_gbq
from shared_utils import geo_utils

credentials, project = google.auth.default()


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


def download_table(
    project_name: str = "cal-itp-data-infra",
    dataset_name: str = "mart_gtfs",
    table_name: str = "",
    date_col: Literal["service_date", "month_first_day", None] = "",
    start_date: str = "",
    end_date: str = "",
    geom_col: str = None,
    geom_type: Literal["point", "line"] = None,
) -> Literal[pd.DataFrame, gpd.GeoDataFrame]:
    """
    Set up a basic query and use pandas_gbq to import.
    Coerce datetime column and convert to gdf if needed.
    """
    basic_query = basic_sql_query(project_name, dataset_name, table_name)
    where_condition = add_sql_date_filter(date_col, start_date, end_date)
    sql_query_statement = f"{basic_query} {where_condition}"

    if date_col is None:
        df = pandas_gbq.read_gbq(basic_query, project_id=project_name, dialect="standard", credentials=credentials)

        print(f"query: {basic_query}")

    if date_col is not None:
        df = pandas_gbq.read_gbq(
            sql_query_statement, project_id=project_name, dialect="standard", credentials=credentials
        ).astype({date_col: "datetime64[ns]"})

        print(f"query: {sql_query_statement}")

    if geom_col is not None:

        df = geo_utils.convert_to_gdf(df, geom_col, geom_type)

    return df
