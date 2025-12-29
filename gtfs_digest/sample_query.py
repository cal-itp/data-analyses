# More here: https://github.com/cal-itp/data-analyses/blob/main/gtfs_digest/demo_querying_bq.ipynb
import google.auth
import pandas as pd
import pandas_gbq

from calitp_data_analysis import utils
from typing import Enum
from shared_utils import geo_utils
credentials, project = google.auth.default()

def add_sql_date_filter(
    date_col: str,
    start_date: str,
    end_date: str
) -> str:
    """
    Add a where condition to filter by date, coerce the dates so sql_query is read correctly.
    """
    where_condition = f"WHERE {date_col} >= DATE('{start_date}') AND {date_col} <= DATE('{end_date}')"
    
    return where_condition

def download_gdf_pandas_gbq(
    project: Enum["cal-itp_data-infra", "cal-itp-data-infra-staging"] = "cal-itp-data-infra",
    dataset_name: Enum["mart_gtfs_rollup", "tiffany_mart_gtfs_rollup"] = "mart_gtfs_rollup",
    table_name: str = "",
) -> pd.DataFrame:
    """
    Write out sql_query_statement explicitly here, but
    probably can do with improving the function here for use in a script if several tables are downloaded.
    """
    sql_query_statement =  f"SELECT * FROM  `{project_name}`.`{dataset_name}`.`{table_name}`"

    # this example filters from June 2025 - Nov 2025
    where_condition = add_sql_date_filter("month_first_day", "2025-06-01", "2025-11-01")
  
    df = pandas_gbq.read_gbq(
        f"{sql_query_statement} {where_condition}", 
        project_id = project,
        dialect = "standard",
        credentials = credentials
    ).astype(
       # add this otherwise data type is dbdate and when you read it in, will cause error
      {"month_first_day": "datetime64[ns]"}
    ).pipe(
        geo_utils.convert_to_gdf,
        "pt_geom", # stop geometry column is pt_geom
        "point" # we want to convert that geometry column to a point
    )

    # will need export step, for now just return the results
    #utils.geoparquet_gcs_export(df, ...) # how to insert pyarrow engine?
    #df.to_parquet(engine='pyarrow') must have engine!
    
    return df

def download_pandas_gbq_no_dates(
    project: Enum["cal-itp_data-infra", "cal-itp-data-infra-staging"] = "cal-itp-data-infra",
    dataset_name: Enum["mart_transit_database", "tiffany_mart_transit_database"] = "mart_transit_database",
    table_name: str = "",
) -> pd.DataFrame:
    """
    Can only think of transit_database not necessarily needing the dates in pre-joined tables
    """
    sql_query_statement =  f"SELECT * FROM  `{project_name}`.`{dataset_name}`.`{table_name}`"

    df = pandas_gbq.read_gbq(
        f"{sql_query_statement} {where_condition}", 
        project_id = project,
        dialect = "standard",
        credentials = credentials
    )
    return df

if __name__ == "__main__":
        stops = download_gdf_pandas_gbq(
            project = "cal-itp-data-infra",
            dataset_name = "mart_gtfs_rollup",
            table_name = "fct_monthly_scheduled_stops"
        )

        # This crosswalk gives you gtfs_dataset_name to analysis_name to ntd_id with caltrans_district, RTPA, etc
        # Should be able to go from this to dim_orgs, but may need to tweak to make this possible
        crosswalk = download_pandas_gbq_no_dates(
            project = "cal-itp-data-infra-staging",
            dataset_name = "tiffany_mart_transit_database",
            table_name = "bridge_gtfs_analysis_name_x_ntd"            
        )
