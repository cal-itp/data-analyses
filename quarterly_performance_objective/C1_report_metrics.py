"""
Functions to calculate summary stats for report.

Two reports: current quarter, historical report.
Since we need to the same dataset across notebooks, 
generate the various pieces needed in the report too.
"""
import geopandas as gpd
import intake
import pandas as pd

from typing import Literal

from shared_utils import geography_utils

catalog = intake.open_catalog("*.yml")

def aggregate_calculate_percent_and_average(
    df: pd.DataFrame, 
    group_cols: list,
    sum_cols: list) -> pd.DataFrame:
    """
    Create columns with pct values. 
    """
    agg_df = geography_utils.aggregate_by_geography(
        df, 
        group_cols = group_cols,
        sum_cols = sum_cols,
    )
    
    for c in sum_cols:
        new_col = f"pct_{c}"
        agg_df[new_col] = (agg_df[c] / agg_df[c].sum()).round(3)
        agg_df[c] = agg_df[c].round(0)
        
    return agg_df

#https://stackoverflow.com/questions/23482668/sorting-by-a-custom-list-in-pandas
def sort_by_column(df: pd.DataFrame, 
                   col: str = "category", 
                   sort_key: list = ["on_shn", "intersects_shn", "other"]
                  ) -> pd.DataFrame:
    # Custom sort order for categorical variable
    df = df.sort_values(
        col, key=lambda c: c.map(lambda e: sort_key.index(e)))
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


def get_service_hours_summary_table(df: pd.DataFrame)-> pd.DataFrame: 
    """
    Aggregate by parallel/on_shn/other category.
    Calculate number and pct of service hours, routes.
    """
    
    summary = aggregate_calculate_percent_and_average(
        df,
        group_cols = ["category"],
        sum_cols = ["service_hours", "unique_route"]
    ).astype({"unique_route": int})
    
    summary = sort_by_column(summary).pipe(clean_up_category_values)
    
    summary = summary.assign(
        service_hours_per_route = round(summary.service_hours / 
                                      summary.unique_route, 2)
    )
    
    return summary


def get_delay_summary_table(df: pd.DataFrame) -> pd.DataFrame:
    # Note: merge_delay both narrows down the dataset quite a bit
    delay_df = df[df.merge_delay=="both"]

    delay_summary = aggregate_calculate_percent_and_average(
        delay_df,
        group_cols = ["category"],
        sum_cols = ["delay_hours", "unique_route"],
    ).astype({"unique_route": int})
    
    delay_summary = (sort_by_column(delay_summary)
                     .pipe(clean_up_category_values)
                    )
        
    delay_summary = delay_summary.assign(
        delay_hours_per_route = round(delay_summary.delay_hours / 
                                      delay_summary.unique_route, 2)
    )
    
    return delay_summary


def by_district_on_shn_breakdown(df: pd.DataFrame,
                                 sum_cols: list) -> pd.DataFrame:
    """
    Get service hours or delay hours by district, and 
    add in percent and average metrics.
    """
    by_district = aggregate_calculate_percent_and_average(
        df[df.category=="on_shn"],
        group_cols = ["district"],
        sum_cols = sum_cols
    ).astype(int).sort_values("district").reset_index(drop=True)
    
    # Calculate average
    if "service_hours" in by_district.columns:
        numerator_col = "service_hours"
    elif "delay_hours" in by_district.columns:
        numerator_col = "delay_hours"
    
    by_district = by_district.assign(
        avg = by_district[numerator_col].divide(
            by_district.unique_route).round(1)
    ).rename(columns = {"avg": f"avg_{numerator_col}"})
    
    return by_district


    
def prep_data_for_report(analysis_date: str) -> gpd.GeoDataFrame:
    # https://stackoverflow.com/questions/69781678/intake-catalogue-level-parameters
    
    df = catalog.routes_categorized(
        analysis_date = analysis_date).read()

    # TODO: Some interest in excluding modes like rail from District 4
    # Don't have route_type...but maybe we want to exclude rail
    '''
    df = df.assign(
        delay_hours = round(df.delay_seconds / 60 ** 2, 2)
    ).drop(columns = "delay_seconds")
    
    df = df[df.merge_delay != "right_only"].reset_index(drop=True)
    '''
    return df



def quarterly_summary_long(analysis_date: str) -> pd.DataFrame: 
    """
    For historical report, get a long df of service hours and delay hours 
    summary tables.
    """
    df = prep_data_for_report(analysis_date)
    
    service_summary = get_service_hours_summary_table(df)                      
    delay_summary = (get_delay_summary_table(df)
                     .rename(columns = {"unique_route": "delay_unique_route"})
                    )
                         
    # Make long
    service_value_vars = [c for c in service_summary.columns if c != "category"]
    delay_value_vars = [c for c in delay_summary.columns if c != "category"]

    service_long = pd.melt(
        service_summary,
        id_vars = "category",
        value_vars = service_value_vars,
    )

    delay_long = pd.melt(
        delay_summary, 
        id_vars = "category", 
        value_vars = delay_value_vars
    )

    # Concatenante
    summary = pd.concat([service_long, delay_long], axis=0)
    summary = summary.assign(
        service_date = analysis_date
    )
    
    return summary
    
    
def district_breakdown_long(analysis_date: str) -> pd.DataFrame: 
    """
    For historical report, get a long df of service hours and delay hours 
    summary tables.
    """
    df = prep_data_for_report(analysis_date)
    
    by_district_summary = by_district_on_shn_breakdown(
        df, sum_cols = ["service_hours", "unique_route"])

    by_district_delay = by_district_on_shn_breakdown(
        df, sum_cols = ["delay_hours", "unique_route"]
    ).rename(columns = {"unique_route": "delay_unique_route"})
                         
    # Make long
    service_value_vars = [c for c in by_district_summary.columns if c != 'district']
    delay_value_vars = [c for c in by_district_delay.columns if c != 'district']

    service_long = pd.melt(
        by_district_summary,
        id_vars = "district",
        value_vars = service_value_vars,
    )

    delay_long = pd.melt(
        by_district_delay, 
        id_vars = "district", 
        value_vars = delay_value_vars
    )

    # Concatenante
    summary = pd.concat([service_long, delay_long], axis=0)
    summary = summary.assign(
        service_date = analysis_date
    )
    
    return summary


def concatenate_summary_across_dates(rt_dates_dict: dict, 
                                     summary_dataset: Literal["summary", "district"],
                                    ) -> pd.DataFrame:
    """
    Loop across dates available for quarterly performance metrics,
    and concatenate into 1 long df.
    """
    df = pd.DataFrame()
    
    rt_dates_reversed = {value: key for key, value in rt_dates_dict.items()}
    
    for date, quarter in rt_dates_reversed.items():
        if summary_dataset == "summary":
            one_quarter = quarterly_summary_long(date)
            
        elif summary_dataset == "district":
            one_quarter = district_breakdown_long(date)
        df = pd.concat([df, one_quarter], axis=0)
    
    df = df.assign(
        year_quarter = df.service_date.map(rt_dates_reversed)
    )

    df = df.assign(
        quarter = df.year_quarter.str.split('_', expand=True)[0],
        year = df.year_quarter.str.split('_', expand=True)[1].astype(int),
    )
    
    # Get it to be year first
    df = df.assign(
        year_quarter = df.year.astype(str) + ' ' + df.quarter
    )
    
    return df