from functools import cache

import geopandas as gpd
import numpy as np
import pandas as pd
from calitp_data_analysis.gcs_geopandas import GCSGeoPandas
from calitp_data_analysis import geography_utils
from shared_utils import publish_utils

from update_vars import GTFS_DATA_DICT, RT_SCHED_GCS

GCS_FILE_PATH = "gs://calitp-analytics-data/data-analyses/state_highway_network/"

"""
Functions
"""

@cache
def gcs_geopandas():
    return GCSGeoPandas()

def process_transit_routes() -> gpd.GeoDataFrame:
    """
    Select the most recent transit route to
    figure out how much of it intersects with
    the state highway network.
    """
    # Load in the route shapes.
    OPERATOR_ROUTE = GTFS_DATA_DICT.digest_tables.operator_routes_map

    subset = [
        "service_date",
        "geometry",
        "portfolio_organization_name",
        "recent_combined_name",
        # "route_id",
    ]

    op_geography_df = gcs_geopandas().read_parquet(f"{RT_SCHED_GCS}{OPERATOR_ROUTE}.parquet")[subset]

    # Keep the row for each portfolio_organization_name/recent_combined_name
    # that is the most recent.
    most_recent_routes = publish_utils.filter_to_recent_date(
        df=op_geography_df,
        group_cols=[
            "portfolio_organization_name",
        ],
    )

    # Calculate the length of route, ensuring that it is in feet.
    most_recent_routes = most_recent_routes.assign(
        route_length_feet=most_recent_routes.geometry.to_crs(
            geography_utils.CA_NAD83Albers_ft
        ).length
    )

    # Drop any duplicates.
    # This will probably be taken out once the 1:m recent_combined_name
    # to route_id issue is resolved.
    most_recent_routes = most_recent_routes.drop_duplicates(
        subset=["portfolio_organization_name", "recent_combined_name", "service_date"]
    )
    return most_recent_routes

def dissolve_shn(columns_to_dissolve: list, file_name: str) -> gpd.GeoDataFrame:
    """
    Dissolve State Highway Network so there will only be one row for each
    route name and route type
    """
    # Read in the dataset and change the CRS to one to feet.
    SHN_FILE = catalog_utils.get_catalog(
        "shared_data_catalog"
    ).state_highway_network.urlpath

    shn = gcs_geopandas().read_parquet(SHN_FILE).to_crs(geography_utils.CA_NAD83Albers_ft)

    # Dissolve by route which represents the the route's name and drop the other columns
    # because they are no longer relevant.
    shn_dissolved = (shn.dissolve(by=columns_to_dissolve).reset_index())[
        columns_to_dissolve + ["geometry"]
    ]

    # Rename because I don't want any confusion between SHN route and
    # transit route.
    shn_dissolved = shn_dissolved.rename(columns={"Route": "shn_route"})

    # Find the length of each highway.
    shn_dissolved = shn_dissolved.assign(
        highway_feet=shn_dissolved.geometry.length,
        shn_route=shn_dissolved.shn_route.astype(int).astype(str),
    )

    # Save this out so I don't have to dissolve it each time.
    shn_dissolved = gcs_geopandas().geo_data_frame_to_parquet(
        shn_dissolved,
        f"gs://calitp-analytics-data/data-analyses/state_highway_network/shn_dissolved_by_{file_name}.parquet"
    )

    return shn_dissolved

def buffer_shn(buffer_amount: int, file_name: str) -> gpd.GeoDataFrame:
    """
    Add a buffer to the SHN before overlaying it with
    transit routes.
    """
    # Read in the dissolved SHN file
    shn_df = gcs_geopandas().read_parquet(f"{GCS_FILE_PATH}shn_dissolved_by_{file_name}.parquet")

    # Buffer the state highway.
    shn_df_buffered = shn_df.assign(
        geometry=shn_df.geometry.buffer(buffer_amount),
    )

    # Save it out so we won't have to buffer over again and
    # can just read it in.
    shn_df_buffered = gcs_geopandas().geo_data_frame_to_parquet(
        shn_df_buffered,
        f"{GCS_FILE_PATH}shn_buffered_{buffer_amount}_ft_{file_name}.parquet"
    )

    return shn_df_buffered

def routes_shn_intersection(buffer_amount: int, file_name: str) -> gpd.GeoDataFrame:
    """
    Overlay the most recent transit routes with a buffered version
    of the SHN
    """
    # GCS_FILE_PATH = "gs://calitp-analytics-data/data-analyses/state_highway_network/"

    # Read in buffered shn here or re buffer if we don't have it available.
    HWY_FILE = f"{GCS_FILE_PATH}shn_buffered_{buffer_amount}_ft_{file_name}.parquet"

    if gcs_geopandas().gcs_filesystem.exists(HWY_FILE):
        shn_routes_gdf = gcs_geopandas().read_parquet(HWY_FILE)
    else:
        shn_routes_gdf = buffer_shn(buffer_amount)

    # Process the most recent transit route geographies and ensure the
    # CRS matches the SHN routes' GDF so the overlay doesn't go wonky.
    transit_routes_gdf = process_transit_routes().to_crs(shn_routes_gdf.crs)

    # Overlay transit routes with the SHN geographies.
    gdf = gpd.overlay(
        transit_routes_gdf, shn_routes_gdf, how="intersection", keep_geom_type=True
    )

    # Calcuate the percent of the transit route that runs on a highway, round it up and
    # multiply it by 100. Drop the geometry because we want the original transit route
    # shapes.
    gdf = gdf.assign(
        pct_route_on_hwy=(gdf.geometry.length / gdf.route_length_feet).round(3) * 100,
    ).drop(
        columns=[
            "geometry",
        ]
    )

    # Join back the dataframe above with the original transit route dataframes
    # so we can have the original transit route geographies.
    gdf2 = pd.merge(
        transit_routes_gdf,
        gdf,
        on=[
            "service_date",
            "portfolio_organization_name",
            "recent_combined_name",
            "route_length_feet",
        ],
        how="left",
    )

    # Clean up
    gdf2.district = gdf2.district.fillna(0).astype(int)
    return gdf2

def group_route_district(df: pd.DataFrame, pct_route_on_hwy_agg: str) -> pd.DataFrame:

    # Aggregate by adding all the districts and SHN to a single row, rather than
    # multiple and sum up the total % of SHN a transit route intersects with
    agg1 = (
        df.groupby(
            [
                "analysis_name",
                "recent_combined_name",
            ],
            as_index=False,
        )[["shn_route", "district", "pct_route_on_hwy_across_districts"]]
        .agg(
            {
                "shn_route": lambda x: ", ".join(set(x.astype(str))),
                "district": lambda x: ", ".join(set(x.astype(str))),
                "pct_route_on_hwy_across_districts": pct_route_on_hwy_agg,
            }
        )
        .reset_index(drop=True)
    )

    # Clean up
    agg1.pct_route_on_hwy_across_districts = (
        agg1.pct_route_on_hwy_across_districts.astype(float).round(2)
    )
    return agg1

def create_on_shs_column(df):
    df["on_shs"] = np.where(df["pct_route_on_hwy_across_districts"] == 0, "N", "Y")
    return df

def prep_open_data_portal(gdf: gpd.GeoDataFrame) -> pd.DataFrame:
    """
    Prepare the gdf to join with the existing transit_routes
    dataframe that is published on the Open Data Portal
    """
    # Rename column
    gdf = gdf.rename(columns={"pct_route_on_hwy": "pct_route_on_hwy_across_districts"})
    # Group the dataframe so that one route only has one
    # row instead of multiple rows after finding its
    # intersection with any SHN routes.
    agg1 = group_route_district(gdf, "sum")

    # Add yes/no column to signify if a transit route intersects
    # with a SHN route
    agg1 = create_on_shs_column(agg1)

    return agg1

def dissolve_buffered_for_map(buffer_amount: str) -> gpd.GeoDataFrame:
    # GCS_FILE_PATH = "gs://calitp-analytics-data/data-analyses/state_highway_network/"
    # Read in buffered shn here
    HWY_FILE = (
        f"{GCS_FILE_PATH}shn_buffered_{buffer_amount}_ft_ct_district_route.parquet"

    )
    gdf = gcs_geopandas().read_parquet(HWY_FILE)

    # Dissolve by district
    gdf2 = gdf.dissolve("district").reset_index()[["geometry", "district", "shn_route"]]
    # Save
    gcs_geopandas().geo_data_frame_to_parquet(
        gdf2,
        f"{GCS_FILE_PATH}shn_buffered_{buffer_amount}_gtfs_digest.parquet"
    )
    
def final_transit_route_shs_outputs(
    intersecting_gdf: gpd.GeoDataFrame,
    open_data_df: pd.DataFrame,
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
    # Filter out for any pct_route_on_hwy that we deem too low & for the relevant district.
    open_data_df = open_data_df.loc[
        (open_data_df.pct_route_on_hwy_across_districts > pct_route_intersection)
        & (open_data_df.district.str.contains(district))
    ]
    # intersecting_gdf.district = intersecting_gdf.district
    intersecting_gdf = intersecting_gdf.loc[
        intersecting_gdf.district.astype(str).str.contains(district)
    ]

    # Join back to get the original transit route geometries and the names of the
    # state highways these routes intersect with. This gdf will be used to
    # display a map.
    map_gdf = pd.merge(
        intersecting_gdf[
            ["portfolio_organization_name", "recent_combined_name", "geometry"]
        ].drop_duplicates(),
        open_data_df,
        on=["portfolio_organization_name", "recent_combined_name"],
    )

    # Add column for color scale when mapping
    # map_gdf = categorize_percentiles(map_gdf)

    # We want a text table to display.
    # Have to rejoin and to find only the SHN routes that are in the district
    # we are interested in.
    text_table_df = pd.merge(
        intersecting_gdf[
            [
                "portfolio_organization_name",
                "recent_combined_name",
                "shn_route",
                "district",
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
    text_table = group_route_district(text_table_df, "max")

    # Rename for clarity
    text_table = text_table.rename(
        columns={
            "shn_route": f"shn_routes_in_d_{district}",
        }
    )

    return map_gdf, text_table

if __name__ == "__main__":
    SHN_HWY_BUFFER_FEET = 50
    
    # Load recent transit routes
    recent_transit_routes = process_transit_routes()
    
    # Intersect with SHN
    intersection_gdf = routes_shn_intersection(SHN_HWY_BUFFER_FEET, "ct_district_route")
    
    # Dataframe for the Open Data Portal
    open_data_portal_df = prep_open_data_portal(intersection_gdf)
    
    # Save everything out for now
    gcs_geopandas().geo_data_frame_to_parquet(
        intersection_gdf,
        f"{GCS_FILE_PATH}transit_route_intersect_shn_{SHN_HWY_BUFFER_FEET}_gtfs_digest.parquet"
    )

    gcs_geopandas().geo_data_frame_to_parquet(
        open_data_portal_df,
        f"{GCS_FILE_PATH}transit_route_shn_open_data_portal_{SHN_HWY_BUFFER_FEET}.parquet"
    )
    print("successfully saved out results.")
