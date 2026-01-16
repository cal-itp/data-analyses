import geopandas as gpd
import pandas as pd
from shared_utils import catalog_utils, webmap_utils
from update_vars import GTFS_DATA_DICT, RT_SCHED_GCS, file_name
from calitp_data_analysis import geography_utils

import google.auth
credentials, project = google.auth.default()
import gcsfs
fs = gcsfs.GCSFileSystem()

from functools import cache
from calitp_data_analysis.gcs_pandas import GCSPandas

@cache
def gcs_pandas():
    return GCSPandas()
    
"""
Column Names
"""
transit_shn_map_columns = {
    "analysis_name": "Analysis Name",
    "recent_combined_name": "Route",
    "shn_route": "State Highway Network Route",
    "pct_route_on_hwy_across_districts": "Percentage of Transit Route on SHN Across All Districts",
}


shn_map_readable_columns = {"shn_route": "State Highway Network Route",
                           "district":"District"}


"""
Prep Data
"""
def prep_gdf(gdf:gpd.GeoDataFrame)->gpd.GeoDataFrame:
    gdf = (gdf.to_crs(geography_utils.CA_NAD83Albers_m)
    .drop(columns = [ 'Year', 'Month', 'Month First Day'])
                       )

    gdf = gdf.dissolve(by = "Analysis Name").reset_index()
    
    gdf = gdf.reset_index(drop=False)
    gdf = gdf.rename(columns={"index": "Number"})
    return gdf


def create_operator_table(df:pd.DataFrame)->pd.DataFrame:
    cols_to_keep = ["Analysis Name",
                    'Daily Trips',
                    'N Routes',
                    'N Shapes', 
                    'N Stops', ]
    df2 = df[cols_to_keep]
    df2 = df2.rename(columns = {"Analysis Name":"Operator"})
    df2.columns = df2.columns.str.replace("N","#")
    return df2

"""
Reshape
"""
def transpose_summary_stats(
    df: pd.DataFrame, 
    district_col: str = "Caltrans District"
) -> pd.DataFrame:
    """
    District summary should be transposed, otherwise columns
    get shrunk and there's only 1 row.
    
    Do some wrangling here so that great tables
    can display it fairly cleanly.
    """
    # Fix this so we can see it
    subset_df = df.drop(
        columns = district_col
    ).reset_index(drop=True)
    
    subset_df2 = subset_df.rename(
        columns = {
            **{c: f"{c.replace('N', '# ')}" for c in subset_df.columns},
            "n_operators": "# Operators",
            "arrivals_per_stop": "Arrivals per Stop",
            "trips_per_operator": "Trips per Operator"
        }).T.reset_index().rename(columns = {0: "Value", "index": "Category"})
    
    # Change to string for display
    subset_df2['Value'] = subset_df2['Value'].astype(int).apply(lambda x: "{:,}".format(x))
    return subset_df2


def create_summary_table(df:pd.DataFrame)->pd.DataFrame:
    sum_me = [
    'N Trips',
    'N Stops',
    'N Routes',
    ]

    agg1 = (df.groupby(['Caltrans District'], 
                      observed=True, group_keys=False)
           .agg({
               "Analysis Name": "nunique",
               **{c:"sum" for c in sum_me},
           })
           .reset_index()
           .rename(columns = {"Analysis Name": "N Operators"})
          )

    agg1 = transpose_summary_stats(agg1)
    return agg1


"""
State Highway Network 
"""
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
    open_data_df = gcs_pandas().read_parquet(
    f"{GCS_PATH}transit_route_shn_open_data_portal_50.parquet")

    intersecting_gdf = gpd.read_parquet(
    f"{GCS_PATH}transit_route_intersect_shn_50_gtfs_digest.parquet",
    storage_options={"token": credentials.token})

    FILEPATH_URL = f"{GTFS_DATA_DICT.gcs_paths.DIGEST_GCS}processed/{GTFS_DATA_DICT.gtfs_digest_rollup.crosswalk}_{file_name}.parquet"

    crosswalk_df = (gcs_pandas().read_parquet(
        FILEPATH_URL
    )[["caltrans_district","caltrans_district_int"]]
    .drop_duplicates()
         )
    crosswalk_df.caltrans_district_int = crosswalk_df.caltrans_district_int.astype(int)
    intersecting_gdf.district = intersecting_gdf.district.fillna(0).astype(int)
    # Filter out for any pct_route_on_hwy that we deem too low & for the relevant district.
    open_data_df = open_data_df.loc[
        (open_data_df.pct_route_on_hwy_across_districts >= pct_route_intersection)
    ]
    # TEMP
    intersecting_gdf = pd.merge(intersecting_gdf, crosswalk_df, left_on = ["district"],
                               right_on = ["caltrans_district_int"])
    intersecting_gdf = intersecting_gdf.loc[
        intersecting_gdf.caltrans_district == district
    ]

    
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
    text_table = group_route_district(text_table_df, "max").drop(columns = ["district"])

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

    # Temp district number
    district_int = intersecting_gdf.caltrans_district_int.iloc[0]
    return map_gdf, text_table, district_int