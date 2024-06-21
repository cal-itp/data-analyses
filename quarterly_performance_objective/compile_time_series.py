"""
Get quarterly metrics as a time-series df
to use in report.
"""
import datetime
import geopandas as gpd
import pandas as pd

#from calitp_data_analysis import utils
from segment_speed_utils import time_series_utils
from shared_utils import rt_dates
from update_vars import BUS_SERVICE_GCS

operator_cols = [
    "name", 
    "organization_name", "caltrans_district"
]

district_cols = ["caltrans_district"]

category_cols = ["category", "year_quarter"]


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


def assemble_aggregated_df_with_subtotals(
    df: gpd.GeoDataFrame,
    group_cols: list
) -> gpd.GeoDataFrame:
    """
    Statewide (aggregate across operators) df with service hours and 
    speed metrics.
    Also add subtotals for on_shn + parallel 
    and statewide totals.
    """    
    by_category = aggregated_metrics(
        df, group_cols
    )
    
    shn_categories = ["on_shn", "parallel"]
    
    shn_subtotal_df = aggregated_metrics(
        df[df.category.isin(shn_categories)].assign(
            category = "shn_subtotal"), 
        group_cols
    )
    
    total_df = aggregated_metrics(
        df.assign(
            category = "total"
        ), 
        group_cols
    )    
    
    final_df = pd.concat([
        by_category, 
        shn_subtotal_df,
        total_df], 
        axis=0, ignore_index=True
    )
    
    return final_df


def add_time_series_list_columns(
    df: pd.DataFrame,
    group_cols: list,
    time_series_cols: list,
) -> pd.DataFrame:
    """
    """    
    group_cols2 = [c for c in group_cols if c != "year_quarter"]
    
    list_aggregation = (df.sort_values("year_quarter")
           .groupby(group_cols2)
           .agg({
               **{c: lambda x: list(x) 
                  for c in time_series_cols}
           }).reset_index()
           .rename(columns = {
               **{c: f"{c}_ts" for c in time_series_cols}
           })
    )
    
    df2 = pd.merge(
        df,
        list_aggregation,
        on = group_cols2,
        how = "inner"
    )
    
    return df2
    
if __name__ == "__main__":
    
    start = datetime.datetime.now()
    
    all_dates = rt_dates.y2023_dates + rt_dates.y2024_dates
    
    df = assemble_time_series(all_dates)
    
    time_series_cols = ["service_hours_per_route", "speed_mph"]
    
    operator_df = assemble_aggregated_df_with_subtotals(
        df, operator_cols + category_cols)
    
    operator_df2 = add_time_series_list_columns(
        operator_df, 
        operator_cols + category_cols,
        time_series_cols
    )
    
    operator_df2.to_parquet(
        f"{BUS_SERVICE_GCS}"
        "quarterly_metrics/operator_time_series.parquet"
    )
    
    district_df = assemble_aggregated_df_with_subtotals(
        df, district_cols + category_cols)
    
    district_df2 = add_time_series_list_columns(
        district_df, 
        district_cols + category_cols,
        time_series_cols
    )
    
    district_df2.to_parquet(
        f"{BUS_SERVICE_GCS}"
        "quarterly_metrics/district_time_series.parquet"
    )
    
    statewide_df = assemble_aggregated_df_with_subtotals(
        df, category_cols)
    
    statewide_df2 = add_time_series_list_columns(
        statewide_df, 
        category_cols,
        time_series_cols
    )
    
    statewide_df2.to_parquet(
        f"{BUS_SERVICE_GCS}"
        "quarterly_metrics/statewide_time_series.parquet"
    )
    
    end = datetime.datetime.now()
    print(f"quarterly metrics time-series: {end - start}")