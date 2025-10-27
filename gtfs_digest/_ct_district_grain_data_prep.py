import geopandas as gpd
import pandas as pd
import _transit_routes_on_shn
from shared_utils import catalog_utils, webmap_utils
from update_vars import GTFS_DATA_DICT, RT_SCHED_GCS
from calitp_data_analysis import geography_utils

import google.auth
credentials, project = google.auth.default()
import gcsfs
fs = gcsfs.GCSFileSystem()

operator_route_gdf_readable_columns = {"portfolio_organization_name": "Portfolio Organization Name"}

transit_shn_map_columns = {
    "analysis_name": "Analysis Name",
    "recent_combined_name": "Route",
    "shn_route": "State Highway Network Route",
    "pct_route_on_hwy_across_districts": "Percentage of Transit Route on SHN Across All Districts",
}

shn_map_readable_columns = {"shn_route": "State Highway Network Route",
                           "district":"District"}

gtfs_table_readable_columns = {
    "analysis_name": "Analysis Name",
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
    OPERATOR_PROFILE = GTFS_DATA_DICT.digest_tables.operator_profiles
    
    operator_df = pd.read_parquet(
    f"{RT_SCHED_GCS}{OPERATOR_PROFILE}.parquet",
    )
    
    operator_df2 = operator_df.loc[operator_df.caltrans_district == district]
    
    operator_df2 = operator_df2.sort_values(by = ["service_date"], ascending = False).drop_duplicates(subset = ["analysis_name", "name"])
    return operator_df2
        
def data_wrangling_operator_map(portfolio_organization_names: list) -> gpd.GeoDataFrame:

    OPERATOR_ROUTE = GTFS_DATA_DICT.digest_tables.operator_routes_map

    operator_route_gdf = gpd.read_parquet(
        f"{RT_SCHED_GCS}{OPERATOR_ROUTE}.parquet",
        storage_options={"token": credentials.token},
    )[["analysis_name", "service_date", "recent_combined_name", "geometry"]].to_crs(
        geography_utils.CA_NAD83Albers_m
    )

    # Temp
    # operator_route_gdf = operator_route_gdf.rename(columns = {"portfolio_organization_name":"analysis_name"})
    operator_route_gdf = operator_route_gdf.loc[
        operator_route_gdf.analysis_name.isin(portfolio_organization_names)
    ]

    operator_route_gdf = (
        operator_route_gdf.sort_values(
            ["service_date", "analysis_name", "recent_combined_name"],
            ascending=[False, True, True],
        )
        .drop_duplicates(subset=["analysis_name", "recent_combined_name"])
        .drop(columns=["service_date", "recent_combined_name"])
    )

    operator_route_gdf["analysis_name"] = operator_route_gdf[
        "analysis_name"
    ].str.replace(" Schedule", "")

    operator_route_gdf = operator_route_gdf.rename(
        columns=operator_route_gdf_readable_columns
    )

    # Buffer
    operator_route_gdf.geometry = operator_route_gdf.geometry.buffer(35)
    operator_route_gdf = operator_route_gdf.dissolve(by="analysis_name").reset_index()

    # Need to create a number column in order for webmaps to work
    operator_route_gdf = operator_route_gdf.reset_index(drop=False)
    operator_route_gdf = operator_route_gdf.rename(columns={"index": "Number"})
    return operator_route_gdf

def final_transit_route_shs_outputs(
    pct_route_intersection: int,
    district: int,
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
        (open_data_df.pct_route_on_hwy_across_districts >= pct_route_intersection)
    ]
    
    intersecting_gdf = intersecting_gdf.loc[
        intersecting_gdf.district == district
    ]

    # TEMP
    intersecting_gdf = intersecting_gdf.rename(columns = {"portfolio_organization_name":"analysis_name"})
    open_data_df = open_data_df.rename(columns = {"portfolio_organization_name":"analysis_name"})
    # Join back to get the long gdf with the transit route geometries and the names of the
    # state highways these routes intersect with. This gdf will be used to
    # display a map.
    map_gdf = pd.merge(
        intersecting_gdf[
            ["analysis_name", "recent_combined_name", "geometry"]
        ].drop_duplicates(),
        open_data_df,
        on=["analysis_name", "recent_combined_name"],
    )
    
    # Buffer so we can see stuff and change the CRS
    map_gdf = map_gdf.to_crs(geography_utils.CA_NAD83Albers_m)
    map_gdf.geometry = map_gdf.geometry.buffer(35)
    
    # We want a text table to display.
    # Have to rejoin and to find only the SHN routes that are in the district
    # we are interested in.
    text_table_df = pd.merge(
        intersecting_gdf[
            [
                "analysis_name",
                "recent_combined_name",
                "shn_route",
               "district",
            ]
        ],
        open_data_df[
            [
                "analysis_name",
                "recent_combined_name",
                "pct_route_on_hwy_across_districts",
            ]
        ],
        on=["analysis_name", "recent_combined_name"],
    )

    # Now we have to aggregate again so each route will only have one row with the
    # district and SHN route info delinated by commas if there are multiple values.
    text_table = _transit_routes_on_shn.group_route_district(text_table_df, "max").drop(columns = ["district"])

    # Rename for clarity
    text_table = text_table.rename(
        columns={
            "shn_route": f"State Highway Network Routes in District {district}",
        }
    )

    text_table = text_table.rename(columns = transit_shn_map_columns)
    map_gdf = map_gdf.rename(columns = transit_shn_map_columns).drop(columns = ["on_shs"])
    map_gdf = map_gdf.reset_index(drop=False)
    map_gdf = map_gdf.rename(columns={"index": "Number"})
    #map_gdf = map_gdf[['Analysis Name', 'Route', 'geometry',
    #   'State Highway Network Route', "Number"]]
    return map_gdf, text_table

def create_gtfs_stats(df:pd.DataFrame)->pd.DataFrame:
    
    gtfs_service_cols = [c for c in df.columns if "operator_" in c]
    
    gtfs_table_df = df[gtfs_service_cols + ["analysis_name"]].reset_index(drop=True)
    
    gtfs_table_df = gtfs_table_df.groupby(['analysis_name']).agg("sum").reset_index()
    
    gtfs_table_df = gtfs_table_df.rename(columns=gtfs_table_readable_columns)
    
    gtfs_table_df["Avg Arrivals per Stop"] = gtfs_table_df["# Arrivals"]/gtfs_table_df["# Stops"]
    return gtfs_table_df
    
"""
Functions to load maps
"""
def load_ct_district(district:int)->gpd.GeoDataFrame:
    """
    Load in Caltrans Shape.
    """
    caltrans_url = "https://gis.data.ca.gov/datasets/0144574f750f4ccc88749004aca6eb0c_0.geojson?outSR=%7B%22latestWkid%22%3A3857%2C%22wkid%22%3A102100%7D"
    ca_geojson = (gpd.read_file(caltrans_url)).to_crs(geography_utils.CA_NAD83Albers_m)
    district_geojson = ca_geojson.loc[ca_geojson.DISTRICT == district][["geometry"]]
    
    # Add color column
    district_geojson["color"] = [(58, 25, 79)]
    district_geojson["description"] = f"geometry for district {district}"
    boundary = district_geojson.geometry.iloc[0].boundary 
    district_geojson.geometry = [boundary]
    district_geojson.geometry = district_geojson.geometry.buffer(100)
    return district_geojson

def load_buffered_shn_map(district:int) -> gpd.GeoDataFrame:
    """
    Load buffered and dissolved version of the SHN that we can
    use with the webmaps.
    """
    SHN_FILE = catalog_utils.get_catalog("shared_data_catalog").state_highway_network.urlpath

    gdf = gpd.read_parquet(
        SHN_FILE,
        storage_options={"token": credentials.token},
    ).to_crs(geography_utils.CA_NAD83Albers_m)
    
    # Filter for the relevant district
    gdf2 = gdf.loc[gdf.District == district]
    
    # Dissolve
    gdf2 = gdf2.dissolve(by = ["Route","County","District", "RouteType"]).reset_index().drop(columns = ["Direction"])
    
    # Buffer - make it a bit bigger so we can actually see stuff
    gdf2.geometry = gdf2.geometry.buffer(100)
    
    # Rename the columns
    gdf2 = gdf2.rename(columns = shn_map_readable_columns)
    
    return gdf2