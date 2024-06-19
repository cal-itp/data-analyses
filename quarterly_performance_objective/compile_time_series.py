"""
Get quarterly metrics as a time-series df
to use in report.
"""
import datetime
import geopandas as gpd
import pandas as pd

from calitp_data_analysis import utils
from segment_speed_utils import time_series_utils
from shared_utils import rt_dates
from update_vars import BUS_SERVICE_GCS

operator_cols = [
    "name", 
    "organization_name", "caltrans_district"
]

category_cols = ["category", "year_quarter"]

subtotal_categories_dict = {
    "shn_subtotal": ["on_shn", "parallel"],
    "total": ["on_shn", "parallel", "other"]
}

def assemble_time_series(
    date_list: list
) -> gpd.GeoDataFrame:
    """
    Assemble time-series data and add column showing what
    year-quarter the service_date belongs to.
    We'll aggregate all the available data for each 
    quarter (which could be 1-3 dates).
    """
    df = time_series_utils.concatenate_datasets_across_dates(
        BUS_SERVICE_GCS,
        f"routes_categorized_with_speed",
        date_list,
        data_type = "gdf",
    ) 
    
    df = df.assign(
        year_quarter = (df.service_date.dt.year.astype(str) + 
                        "-Q" + 
                        df.service_date.dt.quarter.astype(str)
                       )
    )
    
    return df


def service_hours_aggregation(
    df: pd.DataFrame,
    group_cols: list
) -> pd.DataFrame:
    """
    Aggregate service hours by some grouping of columns
    and also add service hours per route.
    """
    df2 = (
        df
        .groupby(group_cols, 
                 observed=True, group_keys=False)
        .agg({"service_hours": "sum",
              "route_key": "count",
              "service_date": "nunique"
             })
        .reset_index()
        .rename(columns = {
            "route_key": "n_routes",
            "service_date": "n_dates"
        })
    )
    
    df2 = df2.assign(
        # if we have multiple days, the n_routes counted will reflect that
        service_hours_per_route = df2.service_hours.divide(df2.n_routes).round(2),
        daily_service_hours = df2.service_hours.divide(df2.n_dates),
        daily_routes = df2.n_routes.divide(df2.n_dates).round(0).astype(int)
    )
    
    return df2


def speed_aggregation(
    df: pd.DataFrame,
    group_cols: list
) -> pd.DataFrame:
    """
    Aggregate speeds (wherever route averages are available).
    """
    df2 = (
        df[df.speed_mph.notna()]
        .groupby(group_cols, 
                 observed=True, group_keys=False)
        .agg({"speed_mph": "mean",
              "route_key": "count",
             "service_date": "nunique"
             })
        .reset_index()
        .rename(columns = {
            "route_key": "n_vp_routes",
            "service_date": "n_dates"
        })
    )
    
    df2 = df2.assign(
        speed_mph = df2.speed_mph.round(2),
        daily_vp_routes = df2.n_vp_routes.divide(df2.n_dates).round(0).astype(int)
    ).drop(columns = "n_dates")

    return df2


def aggregated_metrics(
    df: gpd.GeoDataFrame,
    group_cols: list
) -> pd.DataFrame:
    """
    Aggregate metrics by grouping of columns (either
    by operator or statewide).
    """
    service_hours_agg = service_hours_aggregation(df, group_cols)
    speed_agg = speed_aggregation(df, group_cols)
    
    df2 = pd.merge(
        service_hours_agg,
        speed_agg,
        on = group_cols,
        how = "left",
    ).fillna(
        {"n_vp_routes": 0}
    ).astype(
        {"n_vp_routes": "int"}
    )
    
    return df2


def get_dissolved_geometry(
    df: pd.DataFrame, 
    group_cols: list
) -> gpd.GeoDataFrame:
    
    unique_combos = df[
        group_cols + ["route_id", "geometry"]
      ].drop_duplicates(
        subset=group_cols + ["route_id"]
    )
    
    # Simplify geometry for quicker dissolve (25ft)
    unique_combos = unique_combos.assign(
        geometry = unique_combos.geometry.simplify(tolerance=25)
    )
    
    route_geom = unique_combos[
        group_cols + ["geometry"]
    ].dissolve(by=group_cols).reset_index()
    
    return route_geom
    

def get_subtotals(
    df: pd.DataFrame, 
    group_cols: list
) -> dict:
    """
    Add a row that captures the SHN subtotals
    and the total across all categories.
    """
    results = {}
    
    for grp, category_list in subtotal_categories_dict.items():
        subset_df = df.loc[df.category.isin(category_list)].assign(
            category = grp
        )
                
        results[grp] = aggregated_metrics(
            subset_df, 
            group_cols
        )
        
    return results


def assemble_operator_df(
    df: gpd.GeoDataFrame
) -> gpd.GeoDataFrame:
    """
    Operator df with service hours and speed metrics.
    Also add subtotals for on_shn + parallel 
    and operator totals.
    """
    group_cols = operator_cols + category_cols
    
    by_category = aggregated_metrics(
        df, group_cols
    )
    
    subtotal_dfs = get_subtotals(
        df, group_cols
    )
    
    operator_geom = get_dissolved_geometry(
        df, group_cols
    )
    
    by_category_gdf = pd.merge(
        operator_geom,
        by_category,
        on = group_cols,
        how = "inner"
    )
    
    final_df = pd.concat([
        by_category_gdf, 
        subtotal_dfs["shn_subtotal"],
        subtotal_dfs["total"]], 
        axis=0, ignore_index=True
    )
    
    return final_df


def assemble_statewide_df(
    df: gpd.GeoDataFrame
) -> gpd.GeoDataFrame:
    """
    Statewide (aggregate across operators) df with service hours and 
    speed metrics.
    Also add subtotals for on_shn + parallel 
    and statewide totals.
    """
    group_cols = category_cols
    
    by_category = aggregated_metrics(
        df, group_cols
    )
    
    subtotal_dfs = get_subtotals(
        df, group_cols
    )
    
    final_df = pd.concat([
        by_category, 
        subtotal_dfs["shn_subtotal"],
        subtotal_dfs["total"]], 
        axis=0, ignore_index=True
    )
    
    return final_df


def category_wrangling(
    df: pd.DataFrame, 
    col: str = "category", 
    sort_key: list = ["on_shn", "parallel", "other", "shn_subtotal", "total"]
) -> pd.DataFrame:
    """
    Custom sort order for categorical variable
    https://stackoverflow.com/questions/23482668/sorting-by-a-custom-list-in-pandas
    """
    category_values = {
        "on_shn": "On SHN", 
        "parallel": "Intersects SHN",
        "other": "Other",
        "shn_subtotal": "On or Intersects SHN",
        "total": "Total"
    }
    
    df = df.sort_values(
        col, key=lambda c: c.map(lambda e: sort_key.index(e))
    ) 
    
    df = df.assign(
        category = df.category.map(category_values)
    )
    
    return df
    
if __name__ == "__main__":
    
    start = datetime.datetime.now()
    
    all_dates = rt_dates.y2023_dates + rt_dates.y2024_dates
    
    df = assemble_time_series(all_dates)
    
    operator_df = assemble_operator_df(df)

    utils.geoparquet_gcs_export(
        operator_df,
        BUS_SERVICE_GCS,
        "quarterly_metrics/operator_time_series"
    )
    
    statewide_df = assemble_statewide_df(df)

    statewide_df.to_parquet(
        f"{BUS_SERVICE_GCS}"
        "quarterly_metrics/statewide_time_series.parquet"
    )
    
    end = datetime.datetime.now()
    print(f"quarterly metrics time-series: {end - start}")