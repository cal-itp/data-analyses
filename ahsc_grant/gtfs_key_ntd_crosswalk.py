import pandas_gbq
import pandas as pd
import google.auth

credentials, project = google.auth.default()

GCS_FILE_PATH = 'gs://calitp-analytics-data/data-analyses/ahsc_grant/'

def download_table(
    section_name: str,
    table_name: str,
):
    """
    """
    df  = pandas_gbq.read_gbq(
        f"""
        select 
            *
        from `cal-itp-data-infra`.`{section_name}`.`{table_name}`
        """, 
        project_id = 'cal-itp-data-infra',
        dialect = "standard",
        credentials = credentials
    )
    if table_name == "dim_provider_gtfs_data":
        table_name = f"{table_name}_full"
    df.to_parquet(
        f"{GCS_FILE_PATH}ahsc_test/{table_name}.parquet",
        engine = "pyarrow"
    )
    
    return

def download_trips_table(
    section_name: str,
    table_name: str,
):
    """
    """
    df  = pandas_gbq.read_gbq(
        f"""
        select 
            *
        from `cal-itp-data-infra`.`{section_name}`.`{table_name}`
        where service_date IN (DATE '2022-11-30', DATE '2022-12-03', DATE '2022-12-04')
        """, 
        project_id = 'cal-itp-data-infra',
        dialect = "standard",
        credentials = credentials
    )

    df.to_parquet(
        f"{GCS_FILE_PATH}ahsc_test/{table_name}.parquet",
        engine = "pyarrow"
    )
    
    return

def filter_to_valid_dates(df: pd.DataFrame, date_list):
    
    df2 = pd.DataFrame()
    
    for one_date in date_list:
        one_date_df = df[
             (df._valid_from_local <= pd.to_datetime(one_date)) &
             (df._valid_to_local >= pd.to_datetime(one_date))
        ]
        
        df2 = pd.concat([df2, one_date_df], ignore_index=True)
    
    # drop dupes because these dates are close together, we can have dupes
    # but we don't want to miss any combinations
    return df2.drop_duplicates().reset_index(drop=True)

if __name__ == "__main__":
    
    # Go one more step back from dim_organizations
    #download_table("staging", "int_transit_database__organizations_dim")
    
    # get the full version of this table
    # Hypothesis 1: all operators from trips must merge with dim_provider_gtfs_data. 
    # This isn't true right now, we are losing observations.
    # But how? Let's get it from another perspective to check, daily summary
    download_table("mart_transit_database", "dim_provider_gtfs_data") 
   
    download_trips_table("mart_gtfs", "fct_daily_feed_scheduled_service_summary") 
