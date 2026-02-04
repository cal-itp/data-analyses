import geopandas as gpd
import pandas as pd
from shared_utils import catalog_utils, webmap_utils
from update_vars import GTFS_DATA_DICT, RT_SCHED_GCS, file_name, analysis_month, previous_month
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


def load_shn_transit_routes(district:str, pct: int)->gpd.GeoDataFrame:
    OPEN_DATA_GCS = "gs://calitp-analytics-data/data-analyses/open_data/"
    gdf = gpd.read_parquet(f"{OPEN_DATA_GCS}export/ca_transit_routes_{previous_month}.parquet",
                             storage_options={"token": credentials.token})

    # Clean district name because there are some extra spaces
    gdf.district_name = gdf.district_name.str.lstrip().str.replace(r'\s*-\s*', '-', regex=True)

    # Filter
    gdf2 = gdf.loc[
        (gdf.district_name == district) &
        (gdf.shn_route != "not_50ft_from_shn") &
        (gdf.pct_route_on_hwy >= pct)
    ].reset_index(drop=True)

    # Clean the dataframe
    gdf2 = gdf2[[
    "route_name",
    "analysis_name",
    "pct_route_on_hwy",
    "shn_route",
    "geometry"
    ]]

    gdf2 = gdf2.rename(columns = {
    "pct_route_on_hwy":"Percentage of Transit Route on SHN Across All Districts", 
    "shn_route": "State Highway Network Route",
    "analysis_name": "Analysis Name",
    "route_name": "Route Name"
    })
    return gdf2
