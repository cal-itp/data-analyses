import _report_utils
import deploy_portfolio_yaml
from update_vars import GTFS_DATA_DICT, RT_SCHED_GCS
import geopandas as gpd
import pandas as pd
import _transit_routes_on_shn

import google.auth
credentials, project = google.auth.default()
import gcsfs
fs = gcsfs.GCSFileSystem()

operator_route_gdf_readable_columns = {"portfolio_organization_name": "Portfolio Organization Name"}

transit_shn_map_columns = {
    "portfolio_organization_name": "Portfolio Organization Name",
    "recent_combined_name": "Route",
    "shn_route": "State Highway Network Route",
    "pct_route_on_hwy_across_districts": "Percentage of Transit Route on SHN Across All Districts",
}

shn_map_readable_columns = {"shn_route": "State Highway Network Route",}

gtfs_table_readable_columns = {
    "portfolio_organization_name": "Portfolio Organization Name",
    "operator_n_trips": "# Trips",
    "operator_n_stops": "# Stops",
    "operator_n_arrivals": "# Arrivals",
    "operator_route_length_miles": "Operator Service Miles",
    "operator_n_routes": "# Routes",
    "operator_n_shapes": "# Shapes",
    "operator_feeds": "Operator Feeds",
}

def data_wrangling_operator_profile(district:str)->pd.DataFrame:
    """
    Display only values in the column portfolio_organization_names
    that are in the organization grain GTFS Digest. Rename columns.
    """
    OPERATOR_PROFILE_REPORT = GTFS_DATA_DICT.digest_tables.operator_profiles_report
    # OPERATOR_PROFILE = GTFS_DATA_DICT.digest_tables.operator_profiles
    
    #portfolio_organization_names_to_keep = (
    #deploy_portfolio_yaml.generate_operator_grain_yaml(OPERATOR_PROFILE)
    #)[["organization_name"]].drop_duplicates()
    
    #operator_df = pd.read_parquet(
    #f"{RT_SCHED_GCS}{OPERATOR_PROFILE_REPORT}.parquet",
    #filters=[[("caltrans_district", "==", district)]],
    #)
    
    operator_df = pd.read_parquet(
    f"{RT_SCHED_GCS}{OPERATOR_PROFILE_REPORT}.parquet",
    )
    
    #operator_df2 = pd.merge(
    #operator_df,
    #portfolio_organization_names_to_keep,
    #left_on=["portfolio_organization_name"],
    #right_on=["organization_name"],
    #how="inner",)
    
    operator_df2 = operator_df.loc[operator_df.caltrans_district == district]
    
    # operator_df2 = operator_df2.rename(columns = operator_profile_report_readable_columns)
    
    return operator_df2
        
def data_wrangling_operator_map(portfolio_organization_names:list)->gpd.GeoDataFrame:
     
    OPERATOR_ROUTE = GTFS_DATA_DICT.digest_tables.operator_routes_map   
    
    operator_route_gdf = gpd.read_parquet(
    f"{RT_SCHED_GCS}{OPERATOR_ROUTE}.parquet",
    storage_options={"token": credentials.token},
)[["portfolio_organization_name", "service_date", "recent_combined_name", "geometry"]]
        
    operator_route_gdf = operator_route_gdf.loc[
    operator_route_gdf.portfolio_organization_name.isin(portfolio_organization_names)]
    
    operator_route_gdf = (
    operator_route_gdf.sort_values(
        ["service_date", "portfolio_organization_name", "recent_combined_name"],
        ascending=[False, True, True],
    )
    .drop_duplicates(subset=["portfolio_organization_name", "recent_combined_name"])
    .drop(
        columns=["service_date", "recent_combined_name"]
        # drop route because after the dissolve, all operator routes are combined
        # so route would hold only the first row's value
    )
    .dissolve(by="portfolio_organization_name").reset_index()
)
    
    operator_route_gdf["portfolio_organization_name"] = operator_route_gdf[
    "portfolio_organization_name"
].str.replace(" Schedule", "")
    
    operator_route_gdf = operator_route_gdf.rename(columns = operator_route_gdf_readable_columns)
    return operator_route_gdf

def final_transit_route_shs_outputs(
    pct_route_intersection: int,
    district: str,
):
    """
    Take the dataframes from prep_open_data_portal and routes_shn_intersection.
    Prepare them for display on the GTFS Caltrans District Digest.

    intersecting_gdf: geodataframe created by
    open_data_df: dataframe created by
    pct_route_intersection: cutoff of the % of the transit route intersecting with the SHN
    district: the Caltrans district we are interested in.
    """
    GCS_PATH = "gs://calitp-analytics-data/data-analyses/state_highway_network/"
    open_data_df = pd.read_parquet(
    f"{GCS_PATH}transit_route_shn_open_data_portal_50.parquet")

    intersecting_gdf = gpd.read_parquet(
    f"{GCS_PATH}transit_route_intersect_shn_50_gtfs_digest.parquet",
    storage_options={"token": credentials.token})

    # Filter out for any pct_route_on_hwy that we deem too low & for the relevant district.
    open_data_df = open_data_df.loc[
        (open_data_df.pct_route_on_hwy_across_districts > pct_route_intersection)
        & (open_data_df.District.str.contains(district))
    ]
    intersecting_gdf = intersecting_gdf.loc[
        intersecting_gdf.District.astype(str).str.contains(district)
    ]

    # Join back to get the long gdf with the transit route geometries and the names of the
    # state highways these routes intersect with. This gdf will be used to
    # display a map.
    map_gdf = pd.merge(
        intersecting_gdf[
            ["portfolio_organization_name", "recent_combined_name", "geometry"]
        ].drop_duplicates(),
        open_data_df,
        on=["portfolio_organization_name", "recent_combined_name"],
    )


    # We want a text table to display.
    # Have to rejoin and to find only the SHN routes that are in the district
    # we are interested in.
    text_table_df = pd.merge(
        intersecting_gdf[
            [
                "portfolio_organization_name",
                "recent_combined_name",
                "shn_route",
               "District",
            ]
        ],
        open_data_df[
            [
                "portfolio_organization_name",
                "recent_combined_name",
                "pct_route_on_hwy_across_districts",
            ]
        ],
        on=["portfolio_organization_name", "recent_combined_name"],
    )

    # Now we have to aggregate again so each route will only have one row with the
    # district and SHN route info delinated by commas if there are multiple values.
    text_table = _transit_routes_on_shn.group_route_district(text_table_df, "max").drop(columns = ["District"])

    # Rename for clarity
    text_table = text_table.rename(
        columns={
            "shn_route": f"State Highway Network Routes in District {district}",
        }
    )

    text_table = text_table.rename(columns = transit_shn_map_columns)
    map_gdf = map_gdf.rename(columns = transit_shn_map_columns).drop(columns = ["on_shs"])
    
    return map_gdf, text_table

def create_gtfs_stats(df:pd.DataFrame)->pd.DataFrame:
    shared_cols = ["portfolio_organization_name"]
    exclude_cols = [
    "schedule_gtfs_dataset_key",
    "caltrans_district",
    "organization_source_record_id",
    "service_date",
    "primary_uza",]

    gtfs_service_cols = [c for c in df.columns if "operator_" in c]
    
    gtfs_table_df = df[shared_cols + gtfs_service_cols].reset_index(drop=True)
    
    gtfs_table_df = gtfs_table_df.rename(columns=gtfs_table_readable_columns)
    
    gtfs_table_df["Avg Arrivals per Stop"] = gtfs_table_df["# Arrivals"]/gtfs_table_df["# Stops"]
    
    # string_cols = gtfs_table_df.select_dtypes(include="object").columns.tolist()
    
    return gtfs_table_df
"""
Functions to load maps
"""
def load_ct_district(district:int)->gpd.GeoDataFrame:
    """
    Load in Caltrans Shape.
    """
    caltrans_url = "https://gis.data.ca.gov/datasets/0144574f750f4ccc88749004aca6eb0c_0.geojson?outSR=%7B%22latestWkid%22%3A3857%2C%22wkid%22%3A102100%7D"
    ca_geojson = (gpd.read_file(caltrans_url)
               .to_crs(epsg=3310))
    district_geojson = ca_geojson.loc[ca_geojson.DISTRICT == district]
    return district_geojson

def load_buffered_shn_map(buffer_amount:int, district:int) -> gpd.GeoDataFrame:
    """
    Overlay the most recent transit routes with a buffered version
    of the SHN
    """
    GCS_FILE_PATH = "gs://calitp-analytics-data/data-analyses/state_highway_network/"

    # Read in buffered shn here or re buffer if we don't have it available.
    HWY_FILE = f"{GCS_FILE_PATH}shn_buffered_{buffer_amount}_gtfs_digest.parquet"
    shn_routes_gdf = gpd.read_parquet(
            HWY_FILE, storage_options={"token": credentials.token}
        )
    
    # Clean
    shn_routes_gdf = shn_routes_gdf.loc[shn_routes_gdf.District == district]
    shn_routes_gdf = shn_routes_gdf.rename(columns = shn_map_readable_columns)
    return shn_routes_gdf