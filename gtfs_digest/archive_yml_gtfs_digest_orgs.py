"""
Create a diagnostic yaml and get some
basic operator counts for the various dfs used,
as well as flag potential red flags.
"""
import numpy as np
import pandas as pd
import pyaml

from update_vars import GTFS_DATA_DICT, RT_SCHED_GCS

DIAGNOSTIC_YAML_PATH = "./diagnostics.yml"

def case1_operator_counts_by_data(
    df: pd.DataFrame,
    count_col: str
) -> pd.DataFrame:
    """
    Count the number of operators for the last 6 available dates.
    We can count by nunique gtfs_dataset_name or portfolio_organization_name
    to see how those counts look.
    """
    last6_dates = sorted(list(df.service_date.unique()))[-6:]
    
    df2 = (
        df[df.service_date.isin(last6_dates)]
        .groupby("service_date")
        .agg({count_col: "nunique"})
        .sort_values("service_date", ascending=False)
        .reset_index()
    )

    return df2


def case2_lost_rt(df: pd.DataFrame) -> pd.DataFrame:
    """
    Find operators that had RT data before, but their most recent data
    was schedule_only.
    """
    group_cols = ["name", "analysis_name"]
    
    df2 = (
        df
        .groupby(group_cols + ["sched_rt_category"])
        .agg({"service_date": "max"})
        .reset_index()
    )
    
    # Pivot this so goes from long to wide
    # Have columns be called the category name (schedule_only or schedule_and_vp)
    # and the values of that column be the date
    # filter to find dates where schedule only happens after schedule_and_vp,
    # which indicates we lost RT
    df3 = df2.pivot(
        index = group_cols, 
        columns = "sched_rt_category", 
        values = "service_date"
    ).query(
        'schedule_only > schedule_and_vp'
    ).reset_index().astype(str)
    
    
    return df3

    
def case3_stale_data(df: pd.DataFrame) -> pd.DataFrame:
    """
    Find where we haven't found operators for at least 3 months.
    https://stackoverflow.com/questions/42822768/pandas-number-of-months-between-two-dates/42822819
    """
    group_cols = ["name", "analysis_name"]
    
    MAX_DATE = df.service_date.max()
        
    df2 = (
        df
        .groupby(group_cols)
        .agg({"service_date": "max"})
        .reset_index()
    )    
    
    # numpy gives decimal places, like 2.3 months difference, which can be useful
    df2 = df2.assign(
        months_stale =  (df2.service_date - MAX_DATE) / np.timedelta64(1, 'M')
    )
    
    df3 = df2[
        df2.months_stale >= 3
    ][group_cols + ["service_date"]].astype(str).reset_index(drop=True)
    
    return df3


def df_to_dict(df: pd.DataFrame) -> list[dict]:
    """
    Return a df with more than 2 columns as a dict.
    This gives us a list with each operator's info formatted
    as a nested dict. 
    """
    return df.to_dict(orient="records")


if __name__ == "__main__":
    
    DIGEST_RT_SCHED = GTFS_DATA_DICT.digest_tables.monthly_route_schedule_vp
    OPERATOR_PROFILE = GTFS_DATA_DICT.digest_tables.operator_profiles
    OPERATOR_ROUTE = GTFS_DATA_DICT.digest_tables.operator_routes_map
    
    route_df = pd.read_parquet(
        f"{RT_SCHED_GCS}{DIGEST_RT_SCHED}.parquet",
        columns = ["name", "analysis_name", "service_date"]
    ).drop_duplicates().reset_index(drop=True).astype(str) 
    
    operator_df = pd.read_parquet(
        f"{RT_SCHED_GCS}{OPERATOR_PROFILE}.parquet",
        columns = [
            "name", "analysis_name", "service_date",
            "sched_rt_category"
        ]
    ).drop_duplicates().reset_index(drop=True)   

    operator_gdf = pd.read_parquet(
        f"{RT_SCHED_GCS}{OPERATOR_ROUTE}.parquet",
        columns = ["name", "analysis_name", "service_date"]
    ).drop_duplicates().reset_index(drop=True).astype(str)
    
    # Count operators from the route-direction grain
    n_operators = case1_operator_counts_by_data(
        route_df, "name"
    ).pipe(df_to_dict)
    
    n_portfolio_operators = case1_operator_counts_by_data(
        route_df, "analysis_name"
    ).pipe(df_to_dict)
    
    n_operators_in_map = case1_operator_counts_by_data(
        operator_gdf, "name"
    ).pipe(df_to_dict)
    
    # Get operators that had RT before, but recently, had only schedule
    lost_rt_operators = case2_lost_rt(operator_df).pipe(df_to_dict)
    
    # Get operators that haven't shown up in 3 months or more
    stale_operators = case3_stale_data(operator_df).pipe(df_to_dict)
    
    error_output = {
        "operators_last6_months": [*n_operators],
        "portfolio_groupings_last6_months": [*n_portfolio_operators],
        "operators_map_last6_months": [*n_operators_in_map],
        "lost_rt" : [*lost_rt_operators],
        "stale_operators": [*stale_operators]
    }

    # Save out the categories of errors into a yaml
    output = pyaml.dump(error_output)

    with open(DIAGNOSTIC_YAML_PATH, "w") as f:
         f.write(output)