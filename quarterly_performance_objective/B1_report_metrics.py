"""
Functions to calculate summary stats for report.
"""
import geopandas as gpd
import pandas as pd

from shared_utils import geography_utils


def add_percent(df: pd.DataFrame, col_list: list) -> pd.DataFrame:
    """
    Create columns with pct values. 
    """
    for c in col_list:
        new_col = f"pct_{c}"
        df[new_col] = (df[c] / df[c].sum()).round(3)
        df[c] = df[c].round(0)
        
    return df

#https://stackoverflow.com/questions/23482668/sorting-by-a-custom-list-in-pandas
def sort_by_column(df: pd.DataFrame, 
                   col: str = "category", 
                   sort_key: list = ["on_shn", "intersects_shn", "other"]
                  ) -> pd.DataFrame:
    # Custom sort order for categorical variable
    df = df.sort_values(col, 
                        key=lambda c: c.map(lambda e: sort_key.index(e)))
    return df


def clean_up_category_values(df: pd.DataFrame) -> pd.DataFrame:
    category_values = {
        "on_shn": "On SHN", 
        "intersects_shn": "Intersects SHN",
        "other": "Other"
    }
    
    df = df.assign(
        category = df.category.map(category_values)
    )
    
    return df


def get_summary_table(df: pd.DataFrame)-> pd.DataFrame: 
    """
    Aggregate by parallel/on_shn/other category.
    Calculate number and pct of service hours, routes.
    """
    summary = geography_utils.aggregate_by_geography(
        df, 
        group_cols = ["category"],
        sum_cols = ["total_service_hours", "unique_route"],
    ).astype({"total_service_hours": int})
    
    summary = add_percent(summary, ["total_service_hours", "unique_route"])
    
    summary = sort_by_column(summary).pipe(clean_up_category_values)
    
    return summary


def seconds_to_hours(df: pd.DataFrame, col: str) -> float:
    return df[col].sum() / 60 **2
  