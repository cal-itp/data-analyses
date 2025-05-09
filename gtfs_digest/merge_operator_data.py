"""
Concatenate the high-level operator stats.
Produce a single row for each operator-date we have.
This comprises first section of GTFS Digest.
"""
import geopandas as gpd
import pandas as pd

from calitp_data_analysis import utils
from calitp_data_analysis.sql import to_snakecase
from segment_speed_utils import time_series_utils
from shared_utils import gtfs_utils_v2, portfolio_utils, publish_utils
from merge_data import merge_in_standardized_route_names, PORTFOLIO_ORGANIZATIONS_DICT
from update_vars import GTFS_DATA_DICT, SCHED_GCS, RT_SCHED_GCS

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

def get_counties() -> gpd.GeoDataFrame:
    """
    Load a geodataframe of the California counties.
    """
    ca_gdf = "https://opendata.arcgis.com/datasets/8713ced9b78a4abb97dc130a691a8695_0.geojson"
    my_gdf = to_snakecase(gpd.read_file(f"{ca_gdf}"))[["county_name", "geometry"]]

    return my_gdf

## TODO: move counties stuff here
# swap order at the bottom since this needs to be created first
def counties_served_by_operator(route_gdf_by_operator):
    """
    take input produced in concatenate_operator_routes
    get counties for operator-date
    df should only be operator-date-counties_served
    use this to merge into crosswalk and replace NTD column
    """
    # Subset
    gdf2 = gdf[["route_id", "service_date", "portfolio_organization_name", "geometry"]]

    # Grab counties
    ca_counties = get_counties()

    # Sjoin
    counties_served = gpd.sjoin(
        gdf2, ca_counties.to_crs(gdf.crs), how="inner", predicate="intersects"
    ).drop(columns="index_right")

    # Drop Duplicates
    counties_served2 = (
        counties_served[["service_date", "portfolio_organization_name", "county_name"]]
        .drop_duplicates()
        .sort_values(by=["county_name"])
        .reset_index(drop=True)
    )

    # Concatenate the counties using a groupby
    counties_served3 = counties_served2.groupby(
        [
            "service_date",
            "portfolio_organization_name",
        ],
        as_index=False,
    ).agg({"county_name": ",".join})
    
    # Rename
    counties_served3 = counties_served3.rename({"county_name":"counties_served"})
    return counties_served3

def concatenate_crosswalks(
    date_list: list
) -> pd.DataFrame:
    """
    Get crosswalk and selected NTD columns for certain dates.
    """
    FILE = GTFS_DATA_DICT.schedule_tables.gtfs_key_crosswalk
    
    ntd_cols = [
        "schedule_gtfs_dataset_key",
        "name",
        "caltrans_district",
        #"counties_served", # remove this and create our own column
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
    ).pipe(
        portfolio_utils.standardize_portfolio_organization_names, 
        PORTFOLIO_ORGANIZATIONS_DICT
    )
    
    
    # Group by name-service_date-portfolio_organization_name to aggregate up to      portfolio_organization_name,because name indicates different feeds, so we want to sum those.
    agg1 = (
    df.groupby(
        [
            "service_date",
            "caltrans_district",
            "portfolio_organization_name",
            "schedule_gtfs_dataset_key",
            "name",
        ]
    )
    .agg({"service_area_pop": "sum", "service_area_sq_miles": "sum"})
    .reset_index()
    )
    
    
    return agg1

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



if __name__ == "__main__":

    from shared_utils import rt_dates
    
    analysis_date_list = (
        rt_dates.y2025_dates + rt_dates.y2024_dates + rt_dates.y2023_dates 
    )
    
    OPERATOR_PROFILE = GTFS_DATA_DICT.digest_tables.operator_profiles
    OPERATOR_ROUTE = GTFS_DATA_DICT.digest_tables.operator_routes_map
    
    public_feeds = gtfs_utils_v2.filter_to_public_schedule_gtfs_dataset_keys()
    
    # Concat operator grain for schedule metrics.
    schedule_df = concatenate_schedule_operator_metrics(analysis_date_list)
    
    # Concat operator grain for rt vs schedule metrics
    rt_schedule_df = concatenate_rt_vs_schedule_operator_metrics(
        analysis_date_list)

    # Concat NTD/crosswalk
    crosswalk_df = concatenate_crosswalks(analysis_date_list)
    
    # Load in scheduled routes.
    gdf = concatenate_operator_routes(
        analysis_date_list
    ).pipe(
        merge_in_standardized_route_names
    ).pipe(
        publish_utils.exclude_private_datasets, 
        col = "schedule_gtfs_dataset_key", 
        public_gtfs_dataset_keys = public_feeds
    )

    # Merge crosswalk_df with counties served that we spatial join on our own
    counties_served_df = counties_served_by_operator(gdf)
    crosswalk_df = pd.merge(crosswalk_df, counties_served_df, how = "left", on = ["portfolio_organization_name", "service_date"])
    
    operator_df = merge_data_sources_by_operator(
        schedule_df,
        rt_schedule_df,
        crosswalk_df
    ).pipe(
        publish_utils.exclude_private_datasets, 
        col = "schedule_gtfs_dataset_key", 
        public_gtfs_dataset_keys = public_feeds
    )
    
    operator_df.to_parquet(
        f"{RT_SCHED_GCS}{OPERATOR_PROFILE}.parquet"
    )
    utils.geoparquet_gcs_export(
        gdf,
        RT_SCHED_GCS,
        OPERATOR_ROUTE
    )