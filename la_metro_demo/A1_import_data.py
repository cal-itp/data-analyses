import pandas as pd

from calitp_data_analysis import get_fs
from calitp_data_analysis.sql import to_snakecase

from shared_utils import rt_dates

fs = get_fs()

analysis_date = rt_dates.DATES["feb2023"]
GCS_FILE_PATH = "gs://calitp-analytics-data/data-analyses/la_metro_demo/"


def clean_column_names(df: pd.DataFrame) -> pd.DataFrame:
    """
    Clean up column names, snakecase, and 
    get rid of special characters.
    """
    df = to_snakecase(df)
    df.columns = df.columns.str.replace("#", "num")
    
    return df


def import_pct_by_division():
    """
    Import percentage of cash taps by division (aggregated).
    """
    FILE = "20221001_20221031_SPAFARE_DIVISIONSTATION_PERCENTAGES.xlsx"
    
    object_path = fs.open(f"{GCS_FILE_PATH}{FILE}")
    df = pd.read_excel(object_path, sheet_name="Sheet1")
    
    df = clean_column_names(df)
    
    df.to_parquet(f"{GCS_FILE_PATH}by_division.parquet")


def import_pct_by_route():
    """
    Import percentage of cash taps by route.
    """
    FILE = "20221001_20221031_SPAFARE_ROUTEID_PERCENTAGES.xlsx"
        
    object_path = fs.open(f"{GCS_FILE_PATH}{FILE}")
    df = pd.read_excel(object_path, sheet_name="Bus Cash")
    
    df = clean_column_names(df).rename(columns = {"line": "route"})
    
    df.to_parquet(f"{GCS_FILE_PATH}by_route.parquet")

    
def import_transactions_by_route_division():
    """
    Import wide dataset of cleaned transactions by route and division.
    Make it long.
    """
    FILE = "Route by Division Breakdown in OCT22 SPAFARE.xlsm"
    
    object_path = fs.open(f"{GCS_FILE_PATH}{FILE}")
    df = pd.read_excel(object_path, skiprows=[0,1,2])
    
    df = clean_column_names(df)
    
    # drop totals
    df = (df[df["unnamed:_0"] != "Total"]
          .drop(columns = ["unnamed:_0", "unnamed:_18"])
          .rename(columns = {"unnamed:_1": "route"})
         )
    
    # make long
    df2 = pd.melt(
        df, 
        id_vars=["route"], 
        var_name = "division", 
        value_name = "num_transactions"
    )

    # In our long df, drop where num_transactions == 0
    df3 = df2[df2.num_transactions > 0].reset_index(drop=True)
    
    
    df3.to_parquet(f"{GCS_FILE_PATH}transactions_by_route_division.parquet")

    
if __name__ == "__main__":
    
    import_pct_by_division()
    import_pct_by_route()
    import_transactions_by_route_division()
                       