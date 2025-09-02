import geopandas as gpd
import pandas as pd
from shared_utils import rt_dates, rt_utils, v1_rt_dates, portfolio_utils
from calitp_data_analysis import geography_utils

SELECTED_DATE = v1_rt_dates.v1_dates["sep2022"]
COMPILED_CACHED_VIEWS = f"{rt_utils.GCS_FILE_PATH}compiled_cached_views/"
GCS_FILE_PATH = 'gs://calitp-analytics-data/data-analyses/regional_bus_network/'


def aggregate_to_route(gdf: gpd.GeoDataFrame) -> gpd.GeoDataFrame:
    """
    Aggregate trip-level df to route-level.
    Route-level does still have multiple origin/destination pairs.
    """
    route_cols = ["calitp_itp_id", "route_id", 
                  "route_type", 
                  "origin_stop_name", "destination_stop_name"
                 ]
    
    num_trips_by_route = aggregate_by_geography(
        gdf,
        group_cols = route_cols,
        nunique_cols = ["trip_id"],
        rename_cols = True
    )
    
    route_df = attach_geometry(
        num_trips_by_route,
        gdf[route_cols + ["origin", "destination", "geometry"]].drop_duplicates(),
        merge_col = route_cols,
        join = "inner"
    )
    
    return route_df

#  grab old function from 90fe8e1 shared_utils.geography_utils
def aggregate_by_geography(
    df: pd.DataFrame | gpd.GeoDataFrame,
    group_cols: list,
    sum_cols: list = [],
    mean_cols: list = [],
    count_cols: list = [],
    nunique_cols: list = [],
    rename_cols: bool = False,
) -> pd.DataFrame:
    """
    df: pandas.DataFrame or geopandas.GeoDataFrame.,
        The df on which the aggregating is done.
        If it's a geodataframe, it must exclude the tract's geometry column

    group_cols: list.
        List of columns to do the groupby, but exclude geometry.
    sum_cols: list.
        List of columns to calculate a sum with the groupby.
    mean_cols: list.
        List of columns to calculate an average with the groupby
        (beware: may want weighted averages and not simple average!!).
    count_cols: list.
        List of columns to calculate a count with the groupby.
    nunique_cols: list.
        List of columns to calculate the number of unique values with the groupby.
    rename_cols: boolean.
        Defaults to False. If True, will rename columns in sum_cols to have suffix `_sum`,
        rename columns in mean_cols to have suffix `_mean`, etc.

    Returns a pandas.DataFrame or geopandas.GeoDataFrame (same as input).
    """
    final_df = df[group_cols].drop_duplicates().reset_index()

    def aggregate_and_merge(
        df: pd.DataFrame | gpd.GeoDataFrame,
        final_df: pd.DataFrame,
        group_cols: list,
        agg_cols: list,
        AGGREGATE_FUNCTION: str,
    ):

        agg_df = df.pivot_table(
            index=group_cols, values=agg_cols, aggfunc=AGGREGATE_FUNCTION
        ).reset_index()

        if rename_cols is True:
            # https://stackoverflow.com/questions/34049618/how-to-add-a-suffix-or-prefix-to-each-column-name
            # Why won't .add_prefix or .add_suffix work?
            for c in agg_cols:
                agg_df = agg_df.rename(columns={c: f"{c}_{AGGREGATE_FUNCTION}"})

        final_df = pd.merge(final_df, agg_df, on=group_cols, how="left", validate="1:1")
        return final_df

    if len(sum_cols) > 0:
        final_df = aggregate_and_merge(df, final_df, group_cols, sum_cols, "sum")

    if len(mean_cols) > 0:
        final_df = aggregate_and_merge(df, final_df, group_cols, mean_cols, "mean")

    if len(count_cols) > 0:
        final_df = aggregate_and_merge(df, final_df, group_cols, count_cols, "count")

    if len(nunique_cols) > 0:
        final_df = aggregate_and_merge(
            df, final_df, group_cols, nunique_cols, "nunique"
        )

    return final_df.drop(columns="index")
    
def attach_geometry(
    df: pd.DataFrame,
    geometry_df: gpd.GeoDataFrame,
    merge_col: list = ["Tract"],
    join: str = "left",
) -> gpd.GeoDataFrame:
    """
    df: pandas.DataFrame
        The df that needs tract geometry added.
    geometry_df: geopandas.GeoDataFrame
        The gdf that supplies the geometry.
    merge_col: list.
        List of columns to do the merge on.
    join: str.
        Specify whether it's a left, inner, or outer join.

    Returns a geopandas.GeoDataFrame
    """
    gdf = pd.merge(
        geometry_df.to_crs(geography_utils.WGS84),
        df,
        on=merge_col,
        how=join,
    )

    return gdf