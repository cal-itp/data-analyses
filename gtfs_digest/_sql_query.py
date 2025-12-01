import datetime
import google.auth
import pandas_gbq
from google.cloud import bigquery
credentials, project = google.auth.default()

def basic_sql_query(
    project,
    filename,
    start_date = "2025-01-01"
):
    dataset_name = filename.split(".")[0]
    table_name = filename.split(".")[1]
    
    query_sql = f"""
        SELECT 
            *
        FROM `{project}`.`{dataset_name}`.`{table_name}`
        WHERE month_first_day >=  DATE('{start_date}')
    """
    return query_sql

def download_with_pandas_gbq(
    project = "cal-itp-data-infra",
    filename: str = "",
):
    start = datetime.datetime.now()

    query_sql_statement = basic_sql_query(project, filename)
    print(query_sql_statement)
    
    df = pandas_gbq.read_gbq(
        query_sql_statement, 
        project_id = project,
        dialect = "standard",
        credentials = credentials
    ).astype({"month_first_day": "datetime64[ns]"}) 
    # add this otherwise data type is dbdate and when you read it in, will cause error
    
    end = datetime.datetime.now()
    
    print(f"download time: {end - start}")
    
    #df.to_parquet(engine='pyarrow') must have engine!
    
    return df