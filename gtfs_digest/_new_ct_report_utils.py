from functools import cache

import geopandas as gpd
import pandas as pd
from calitp_data_analysis import geography_utils
from calitp_data_analysis.gcs_geopandas import GCSGeoPandas
from calitp_data_analysis.gcs_pandas import GCSPandas
from shared_utils import catalog_utils
from update_vars import SHARED_GCS


@cache
def gcs_pandas():
    return GCSPandas()


@cache
def gcs_geopandas():
    return GCSGeoPandas()


"""
Column Names
"""
transit_shn_map_columns = {
    "analysis_name": "Analysis Name",
    "recent_combined_name": "Route",
    "shn_route": "State Highway Network Route",
    "pct_route_on_hwy_across_districts": "Percentage of Transit Route on SHN Across All Districts",
}


shn_map_readable_columns = {"shn_route": "State Highway Network Route", "district": "District"}


"""
Prep Data
"""


def prep_gdf(gdf: gpd.GeoDataFrame) -> gpd.GeoDataFrame:
    gdf = gdf.to_crs(geography_utils.CA_NAD83Albers_m).drop(columns=["Year", "Month", "Month First Day"])

    # gdf = gdf.dissolve(by = "Analysis Name").reset_index()

    gdf = gdf.reset_index(drop=False)
    gdf = gdf.rename(columns={"index": "Number"})
    return gdf


def create_operator_table(df: pd.DataFrame) -> pd.DataFrame:
    cols_to_keep = ["Analysis Name", "Daily Trips", "N Routes", "N Shapes", "N Stops", "Daily Arrivals"]

    df2 = df[cols_to_keep].rename(columns={"Analysis Name": "Operator"})

    # Add new columns and round columns that potentially could have decimals
    df2["Arrivals per Stop"] = df2["Daily Arrivals"].divide(df2["N Stops"]).round(2)
    df2[["Daily Trips", "Daily Arrivals"]] = df2[["Daily Trips", "Daily Arrivals"]].fillna(0).round(0).astype("Int64")

    df2.columns = df2.columns.str.replace("N", "#")
    return df2


"""
Reshape
"""


def transpose_summary_stats(df: pd.DataFrame, district_col: str = "Caltrans District") -> pd.DataFrame:
    """
    District summary should be transposed, otherwise columns
    get shrunk and there's only 1 row.

    Do some wrangling here so that great tables
    can display it fairly cleanly.
    """
    # Fix this so we can see it
    subset_df = df.drop(columns=district_col).reset_index(drop=True)

    subset_df2 = (
        subset_df.rename(
            columns={
                **{c: f"{c.replace('N', '# ')}" for c in subset_df.columns},
                "n_operators": "# Operators",
                "arrivals_per_stop": "Arrivals per Stop",
                "trips_per_operator": "Trips per Operator",
            }
        )
        .T.reset_index()
        .rename(columns={0: "Value", "index": "Category"})
    )

    # Change to string for display
    subset_df2["Value"] = subset_df2["Value"].astype(int).apply(lambda x: "{:,}".format(x))
    return subset_df2


def create_summary_table(df: pd.DataFrame, district_col: str = "Caltrans District") -> pd.DataFrame:
    sum_me = ["N Trips", "N Stops", "N Routes", "Daily Arrivals"]

    agg1 = (
        df.groupby(district_col, observed=True, group_keys=False)
        .agg(
            {
                "Analysis Name": "nunique",
                **{c: "sum" for c in sum_me},
            }
        )
        .reset_index()
        .rename(columns={"Analysis Name": "N Operators"})
    )

    # These need to be calculated again separately
    agg1["Arrivals per Stop"] = agg1["Daily Arrivals"].divide(agg1["N Stops"]).round(2)
    agg1["Trips per Operator"] = agg1["N Trips"].divide(agg1["N Operators"]).round(2)
    agg1[["Daily Trips", "Daily Arrivals"]] = agg1[["Daily Trips", "Daily Arrivals"]].fillna(0).round(0).astype("Int64")

    agg2 = transpose_summary_stats(agg1, district_col)
    return agg2


"""
State Highway Network
"""


def load_ct_district(district: int) -> gpd.GeoDataFrame:
    """
    Load in Caltrans Shape.
    """
    DISTRICT_FILE = f"{SHARED_GCS}caltrans_districts.parquet"

    ca_geojson = (
        gcs_geopandas()
        .read_parquet(
            DISTRICT_FILE,
        )
        .to_crs(geography_utils.CA_NAD83Albers_m)
    )

    district_geojson = ca_geojson.loc[ca_geojson.district == district][["geometry"]]

    # Add color column
    district_geojson["color"] = [(58, 25, 79)]
    district_geojson["description"] = f"geometry for district {district}"
    boundary = district_geojson.geometry.iloc[0].boundary
    district_geojson.geometry = [boundary]
    district_geojson.geometry = district_geojson.geometry.buffer(100)
    return district_geojson


def load_buffered_shn_map(district: int) -> gpd.GeoDataFrame:
    """
    Load buffered and dissolved version of the SHN that we can
    use with the webmaps.
    """
    SHN_FILE = catalog_utils.get_catalog("shared_data_catalog").state_highway_network.urlpath

    gdf = (
        gcs_geopandas()
        .read_parquet(
            SHN_FILE,
        )
        .to_crs(geography_utils.CA_NAD83Albers_m)
    )

    # Filter for the relevant district
    gdf2 = gdf.loc[gdf.District == district]

    # Dissolve
    gdf2 = gdf2.dissolve(by=["Route", "County", "District", "RouteType"]).reset_index().drop(columns=["Direction"])

    # Buffer - make it a bit bigger so we can actually see stuff
    gdf2.geometry = gdf2.geometry.buffer(100)

    # Rename the columns
    gdf2 = gdf2.rename(columns=shn_map_readable_columns)

    return gdf2


def load_shn_transit_routes(district: str, pct: int, month: str) -> gpd.GeoDataFrame:
    OPEN_DATA_GCS = "gs://calitp-analytics-data/data-analyses/open_data/"
    gdf = gcs_geopandas().read_parquet(
        f"{OPEN_DATA_GCS}export/ca_transit_routes_{month}.parquet",
    )

    # Clean district name because there are some extra spaces
    gdf.district_name = gdf.district_name.str.lstrip().str.replace(r"\s*-\s*", "-", regex=True)

    # Filter
    gdf2 = gdf.loc[
        (gdf.district_name == district) & (gdf.shn_route != "not_50ft_from_shn") & (gdf.pct_route_on_hwy >= pct)
    ].reset_index(drop=True)

    # Clean the dataframe
    gdf2 = gdf2[["route_name", "analysis_name", "pct_route_on_hwy", "shn_route", "geometry"]].rename(
        columns={
            "pct_route_on_hwy": "Percentage of Transit Route on SHN Across All Districts",
            "shn_route": "State Highway Network Route",
            "analysis_name": "Analysis Name",
            "route_name": "Route Name",
        }
    )
    return gdf2
