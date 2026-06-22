"""
Import ESRI feature layers from open data portal sources.
Export into shared GCS folder to use across projects.

https://github.com/cagov/caldata-mdsa-caltrans-pems/blob/main/jobs/utils/geo.py
"""

from urllib.parse import parse_qsl, urlencode, urlparse, urlunparse

import geopandas as gpd
import pandas as pd
from calitp_data_analysis import utils
from calitp_data_analysis.sql import to_snakecase

SHARED_GCS = "gs://calitp-analytics-data/data-analyses/shared_data/"

COUNTY_POLYGONS_URL = "https://services1.arcgis.com/jUJYIo9tSA7EHvfZ/arcgis/rest/services/California_County_Boundaries/FeatureServer/0/query"

CALTRANS_DISTRICTS_URL = (
    "https://caltrans-gis.dot.ca.gov/arcgis/rest/services/CHboundary/District_Tiger_Lines/FeatureServer/0/query/"
)

LEGISLATIVE_BASE = "https://services3.arcgis.com/fdvHcZVgB2QSRNkL/arcgis/rest/services/Legislative/FeatureServer/"

LEGISLATIVE_DICT = {
    "ca_assembly_districts": f"{LEGISLATIVE_BASE}0/query/",
    "ca_senate_districts": f"{LEGISLATIVE_BASE}1/query/",
    "ca_congressional_districts": f"{LEGISLATIVE_BASE}2/query/",
}

CALTRANS_BASE = "https://caltrans-gis.dot.ca.gov/arcgis/rest/services/CHhighway/"
CRS_FUNCTIONAL_CLASSICIATION_URL = f"{CALTRANS_BASE}CRS_Functional_Classification/FeatureServer/0/query/"
SHN_LINES_URL = f"{CALTRANS_BASE}SHN_Lines/FeatureServer/0/query/"
SHN_POSTMILES_URL = f"{CALTRANS_BASE}SHN_Postmiles_Tenth/FeatureServer/0/query/"


def gdf_from_esri_feature_service(url):
    """
    Load an Esri Feature Service to a GeoDataFrame.

    Given a URL to an Esri Feature Service, download the features
    as GeoJSON, and put them into a GeoDataFrame.
    """
    parsed = urlparse(url)

    # Ensure we are using the query endpoint of the feature service
    if not parsed.path.endswith("/query"):
        parsed = parsed._replace(path=parsed.path + "/query")

    # Keep grabbing data using the resultOffset until there is no more left
    offset = 0
    gdfs = []
    while True:
        queries = dict(parse_qsl(parsed.query))
        queries.update(
            {
                "where": "1=1",  # Ensure all rows
                "f": "geojson",  # Ensure GeoJSON
                "outFields": "*",  # Ensure all columns
                "resultOffset": str(offset),  # offset the start
                "returnGeometry": "true",  # Yes we want geometries
            }
        )
        offset_url = urlunparse(parsed._replace(query=urlencode(queries)))

        gdf = gpd.read_file(offset_url, driver="GeoJSON")
        if len(gdf) == 0:
            break

        gdfs.append(gdf)
        offset += len(gdf)
    return pd.concat(gdfs).reset_index(drop=True)


def combine_legislative_districts(assembly_districts_url: str, senate_districts_url: str) -> gpd.GeoDataFrame:
    """
    Create a combined assembly district and senate districts
    gdf.
    """
    assembly_districts = gdf_from_esri_feature_service(assembly_districts_url)
    senate_districts = gdf_from_esri_feature_service(senate_districts_url)

    gdf = pd.concat(
        [
            assembly_districts[["AssemblyDistrictLabel", "geometry"]].rename(
                columns={"AssemblyDistrictLabel": "legislative_district"}
            ),
            senate_districts[["SenateDistrictLabel", "geometry"]].rename(
                columns={"SenateDistrictLabel": "legislative_district"}
            ),
        ],
        axis=0,
        ignore_index=True,
    )

    return gdf


def exclude_columns(
    gdf: gpd.GeoDataFrame, list_of_cols: list = ["objectid", "shape__area", "shape__length"]
) -> gpd.GeoDataFrame:
    """
    Drop a couple of columns that tend to show up for ESRI.
    """
    for c in list_of_cols:
        if c in gdf.columns:
            gdf = gdf.drop(columns=c)

    return gdf


if __name__ == "__main__":

    esri_datasets = {
        "ca_county": COUNTY_POLYGONS_URL,
        "caltrans_districts": CALTRANS_DISTRICTS_URL,
        "ca_congressional_districts": LEGISLATIVE_DICT["ca_congressional_districts"],
        "state_highway_network_raw": SHN_LINES_URL,
        "state_highway_network_postmiles": SHN_POSTMILES_URL,
        "public_road_functional_classification": CRS_FUNCTIONAL_CLASSICIATION_URL,
    }
    for dataset_name, url in esri_datasets.items():
        print(dataset_name)
        gdf = gdf_from_esri_feature_service(url)
        gdf = gdf.pipe(to_snakecase).pipe(exclude_columns)

        print(gdf.crs)
        print(gdf.shape)
        utils.geoparquet_gcs_export(gdf, SHARED_GCS, dataset_name)
        del gdf

    legislative_districts_gdf = combine_legislative_districts(
        LEGISLATIVE_DICT["ca_assembly_districts"],
        LEGISLATIVE_DICT["ca_senate_districts"],
    )

    utils.geoparquet_gcs_export(legislative_districts_gdf, SHARED_GCS, "legislative_districts")
