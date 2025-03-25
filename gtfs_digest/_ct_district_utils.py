import geopandas as gpd
import pandas as pd
"""
Functions for building Caltrans
district GTFS Digest report 
(district_report.ipynb).
"""
def ct_district(district:int)->gpd.GeoDataFrame:
    """
    Load in Caltrans Shape.
    """
    caltrans_url = "https://gis.data.ca.gov/datasets/0144574f750f4ccc88749004aca6eb0c_0.geojson?outSR=%7B%22latestWkid%22%3A3857%2C%22wkid%22%3A102100%7D"
    ca_geojson = (gpd.read_file(caltrans_url)
               .to_crs(epsg=3310))
    district_geojson = ca_geojson.loc[ca_geojson.DISTRICT == district]
    return district_geojson