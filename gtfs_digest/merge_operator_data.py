"""
Concatenate the high-level operator stats.
Produce a single row for each operator-date we have.
This comprises first section of GTFS Digest.
"""
import geopandas as gpd
import intake
import pandas as pd

from calitp_data_analysis import utils
from calitp_data_analysis.sql import to_snakecase
from segment_speed_utils import time_series_utils
from shared_utils import gtfs_utils_v2, portfolio_utils, publish_utils
from merge_data import merge_in_standardized_route_names, PORTFOLIO_ORGANIZATIONS_DICT
from update_vars import GTFS_DATA_DICT, SCHED_GCS, RT_SCHED_GCS

catalog = intake.open_catalog("../_shared_utils/shared_utils/shared_data_catalog.yml")
sort_cols = ["schedule_gtfs_dataset_key", "service_date"]

"""
Concatenating Functions 
"""
def concatenate_schedule_operator_metrics(
    date_list: list
) -> pd.DataFrame:
    """
    Get schedule statistics such as number of routes,
    trips, etc. 
    """
    FILE = GTFS_DATA_DICT.schedule_tables.operator_scheduled_stats
    
    df = time_series_utils.concatenate_datasets_across_dates(
        SCHED_GCS,
        FILE,
        date_list,
        data_type = "df",
    ).sort_values(sort_cols).reset_index(drop=True)
    
    return df

def concatenate_rt_vs_schedule_operator_metrics(
    date_list: list
) -> pd.DataFrame:
    """
    Get spatial accuracy and vehicle positions per minute metrics on the
    operator-service_date grain for certain dates.
    """
    FILE = GTFS_DATA_DICT.rt_vs_schedule_tables.vp_operator_metrics
    
    df = time_series_utils.concatenate_datasets_across_dates(
        RT_SCHED_GCS,
        FILE,
        date_list,
        data_type = "df",
    ).sort_values(sort_cols).reset_index(drop=True)
    
    return df

def concatenate_operator_routes( 
    date_list: list
) -> gpd.GeoDataFrame:
    """
    Concatenate operator route gdf (1 representative shape chosen)
    across all dates we have.
    """
    FILE = GTFS_DATA_DICT.schedule_tables.operator_routes
    
    df = time_series_utils.concatenate_datasets_across_dates(
        SCHED_GCS,
        FILE,
        date_list,
        data_type = "gdf",
    ).sort_values(sort_cols).reset_index(drop=True)   
    
    # TODO is there a short/long route, can it be flagged per date as a new column here?
    
    return df


## TODO: move counties stuff here
# swap order at the bottom since this needs to be created first
def counties_served_by_operator(
    route_gdf_by_operator: gpd.GeoDataFrame
) -> gpd.GeoDataFrame:
    """
    take input produced in concatenate_operator_routes, 
    filter down to most recent date per operator,
    spatial join with CA counties boundaries,
    get list of unique CA counties per operator.
    use this to merge into crosswalk and replace NTD column (sometimes incomplete).
    """
    # Grab counties
    ca_counties = catalog.ca_counties.read().pipe(to_snakecase)[
        ["name", "geometry"]
    ].rename(columns = {"name": "counties_served"})

    # operator routes are available for all dates
    # use most recent date per operator and do sjoin
    operator_most_recent_date = publish_utils.filter_to_recent_date(
        route_gdf_by_operator, ["schedule_gtfs_dataset_key"]
    )
    
    operator_recent_routes = pd.merge(
        route_gdf_by_operator[sort_cols + ["route_id", "direction_id", "geometry"]],
        operator_most_recent_date,
        on = sort_cols,
        how = "inner",
    )
    
    # Sjoin most recent route geometries per operator
    # and get unique list of CA counties per operator
    counties_served = gpd.sjoin(
        operator_recent_routes, 
        ca_counties.to_crs(operator_recent_routes.crs), 
        how="inner", 
        predicate="intersects"
    ).groupby(
        "schedule_gtfs_dataset_key", group_keys=False
    ).agg({
        # get the unique counties per operator (set) and sort it alphabetically
        "counties_served": lambda x: list(sorted(set(x)))
    }).reset_index()

    return counties_served


def concatenate_crosswalks(
    date_list: list,
) -> pd.DataFrame:
    """
    Get crosswalk and selected NTD columns for certain dates.
    """
    FILE = GTFS_DATA_DICT.schedule_tables.gtfs_key_crosswalk
    
    ntd_cols = [
        "schedule_gtfs_dataset_key",
        "name",
        "caltrans_district",
        "service_area_sq_miles",
        "hq_city",
        "service_area_pop",
        "organization_type",
        "primary_uza_name",
        "reporter_type"
    ]
        
    df = (
        time_series_utils.concatenate_datasets_across_dates(
            SCHED_GCS,
            FILE,
            date_list,
            data_type="df",
            columns=ntd_cols
        )
        .sort_values(sort_cols)
        .reset_index(drop=True)
    )
    
    df = df.assign(
        caltrans_district = df.caltrans_district.map(
            portfolio_utils.CALTRANS_DISTRICT_DICT
        )
    )#.pipe(
     #   portfolio_utils.standardize_portfolio_organization_names, 
     #   PORTFOLIO_ORGANIZATIONS_DICT
    #)
    
    return df


def shortest_longest_routes(
    df: gpd.GeoDataFrame,
    group_cols: list,
    col_of_interest: str = "route_length_miles"
) -> pd.DataFrame:
    """
    Find the longest and shortest route by miles 
    for each operator.
    """
    shortest = (
        df[df[col_of_interest].notna()] # remove nulls for distance
        .groupby(group_cols, group_keys=False, dropna=False)
        [col_of_interest]
        .min()
        .reset_index()
        .assign(
            shortest_longest = "shortest"
        )
    )

    longest = (
        df[df[col_of_interest].notna()]
        .groupby(group_cols, group_keys=False, dropna=False)
        [col_of_interest]
        .max()    
        .reset_index()
        .assign(
            shortest_longest = "longest"
        )
    )
        
    df2 = pd.concat([shortest, longest], axis=0, ignore_index=True)
    
    return df2


def get_percentile_description(
    percentile_group: str, 
    p25: float, p50: float, p75: float
) -> str:
    """
    Get a long description of where the cut-offs are.
    """
    percentile_dict = {
        "25th percentile": f"25th percentile (<= {int(p25)} miles)",
        "50th percentile": f"26-50th percentile ({int(p25) + 0.1}-{int(p50)} miles)", 
        "< 75th percentile": f"51-75th percentile ({int(p50) + 0.1}-{int(p75)} miles)",
        "> 75th percentile": f"76th percentile (>= {int(p75) + 0.1} miles)",
    }
 
    return percentile_dict[percentile_group]


def categorize_route_percentiles(
    gdf: gpd.GeoDataFrame,
    group_cols: list,
    col_of_interest: str = "route_length_miles"
) -> gpd.GeoDataFrame:
    """
    Take a column you're interested in and categorize it by row
    for each percentile group it belongs to. 
    Also flag the shortest/longest routes.
    """
    # Create columns holding the quartiles for each operator-date
    gdf = gdf.assign(
        p25 = (gdf.groupby(group_cols, group_keys=False, dropna=False)
               [col_of_interest]
               .transform(lambda x: x.quantile(0.25))
              ),
        p50 = (gdf.groupby(group_cols, group_keys=False, dropna=False)
               [col_of_interest]
               .transform(lambda x: x.quantile(0.5))
              ),
        p75 = (gdf.groupby(group_cols, group_keys=False, dropna=False)
               [col_of_interest]
               .transform(lambda x: x.quantile(0.75))
              ),
    ).fillna({
        "p25": 0, 
        "p50": 0, 
        "p75": 0}
    ) # if there are NaNs, fill it in with zero here
    
    # Categorize route into a percentile category
    gdf = gdf.assign(
        # Categorize each route's route_length_miles into a category
        percentile_categorized = gdf.apply(
            lambda x:
            "25th percentile" if x[col_of_interest] <= x.p25 
            else "50th percentile" if (x[col_of_interest] > x.p25 and x[col_of_interest] <= x.p50) 
            else "< 75th percentile" if (x[col_of_interest] > x.p50 and x[col_of_interest] <= x.p75) 
            else "> 75th percentile", axis=1
        ),
    ).rename(columns = {"percentile_categorized": f"{col_of_interest}_percentile"})
    
    gdf = gdf.assign(
        percentile_group = gdf.apply(
            lambda x: 
            get_percentile_description(x.route_length_miles_percentile, x.p25, x.p50, x.p75), 
            axis=1)
    )
    
    # Add shortest and longest routes
    shortest_longest = shortest_longest_routes(gdf, sort_cols)
    
    # Use left merge so that all routes are kept for each date
    # Those that aren't shortest or longest have blanks
    gdf2 = pd.merge(
        gdf,
        shortest_longest,
        on = sort_cols + ["route_length_miles"],
        how = "left"
    ).fillna({"shortest_longest": ""}) 
    
    drop_cols = [
        "p25", "p50", "p75", 
    ]
    
    return gdf2.drop(columns = drop_cols)


def merge_data_sources_by_operator(
    df_schedule: pd.DataFrame,
    df_rt_sched: pd.DataFrame,
    df_crosswalk: pd.DataFrame
) -> pd.DataFrame:
    """
    Merge schedule and rt_vs_schedule data, 
    which are all at operator-date grain.
    This merged dataset will be used in GTFS digest visualizations.
    """    
    df = pd.merge(
        df_schedule,
        df_rt_sched,
        on = sort_cols,
        how = "left",
    ).merge(
        df_crosswalk,
        on = sort_cols + ["name"],
        how = "inner"
    )
        
    return df


def list_pop_as_string(df, stringify_cols: list):
    # pop list as string, so instead of [one, two], we can display "one, two"
    for c in stringify_cols:
        df[c] = df[c].apply(lambda x: ', '.join(map(str, x)))

    return df

def multiple_ntd_info(df):
    """
    Test a function that allows multiple ntd entries, like hq_city, primary_uza, etc.
    Unpack these as a string to populate description.
    Don't think we want to get rid of multiple ntd_ids...if an operator can be associated
    with multiple entries, we should unpack as much as we can? unless we decide to set a primary,
    which is dependent on knowing operator-organization-ntd_id relationship.
    """
    # Group by name-service_date-portfolio_organization_name to aggregate up to 
    # portfolio_organization_name,because name indicates different feeds, so we want to sum those.
    agg1 = (
        df.groupby(
            [
                "service_date",
                "portfolio_organization_name",
                "caltrans_district",
            ]
        )
        .agg({
            "service_area_pop": "sum", 
            "service_area_sq_miles": "sum",
            # do not sort here because we might scramble them?
            "hq_city": lambda x: list(set(x)),
            "reporter_type": lambda x: list(set(x)), 
            "primary_uza_name": lambda x: list(set(x)),
        })
        .reset_index()
    ).pipe(list_pop_as_string, ["hq_city", "reporter_type", "primary_uza_name"])
    
    return agg1
    

if __name__ == "__main__":

    from shared_utils import rt_dates
    
    analysis_date_list = (
        rt_dates.y2025_dates + rt_dates.y2024_dates + rt_dates.y2023_dates 
    )
    
    OPERATOR_PROFILE = GTFS_DATA_DICT.digest_tables.operator_profiles
    OPERATOR_ROUTE = GTFS_DATA_DICT.digest_tables.operator_routes_map
    
    public_feeds = gtfs_utils_v2.filter_to_public_schedule_gtfs_dataset_keys()
    
    # Load in scheduled routes.
    operator_routes = concatenate_operator_routes(
        analysis_date_list
    ).pipe(
        merge_in_standardized_route_names
    ).pipe(
        categorize_route_percentiles, sort_cols
    ).pipe(
        publish_utils.exclude_private_datasets, 
        col = "schedule_gtfs_dataset_key", 
        public_gtfs_dataset_keys = public_feeds
    )  
    
    utils.geoparquet_gcs_export(
        operator_routes,
        RT_SCHED_GCS,
        OPERATOR_ROUTE
    )
    
    # Concat operator grain for schedule metrics.
    schedule_df = concatenate_schedule_operator_metrics(analysis_date_list)
    
    # Concat operator grain for rt vs schedule metrics
    rt_schedule_df = concatenate_rt_vs_schedule_operator_metrics(
        analysis_date_list)

    # Concat NTD/crosswalk
    counties_served = counties_served_by_operator(operator_routes)
    crosswalk_df = concatenate_crosswalks(analysis_date_list)
        
    operator_df = merge_data_sources_by_operator(
        schedule_df,
        rt_schedule_df,
        crosswalk_df
    ).merge(
        counties_served,
        on = "schedule_gtfs_dataset_key",
        how = "inner"
    ).pipe(
        publish_utils.exclude_private_datasets, 
        col = "schedule_gtfs_dataset_key", 
        public_gtfs_dataset_keys = public_feeds
    )
    
    operator_df.to_parquet(
        f"{RT_SCHED_GCS}{OPERATOR_PROFILE}.parquet"
    )
    