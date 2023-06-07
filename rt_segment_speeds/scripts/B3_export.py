"""
Attach columns needed for publishing to open data portal.
"""
import geopandas as gpd
import pandas as pd

from shared_utils import portfolio_utils


def spatial_join_to_caltrans_districts(
    gdf: gpd.GeoDataFrame
) -> gpd.GeoDataFrame: 
    """
    Spatial join any gdf to Caltrans districts (open data portal)
    """
    URL = ("https://caltrans-gis.dot.ca.gov/arcgis/rest/services/CHboundary/"
           "District_Tiger_Lines/FeatureServer/0/query?"
           "outFields=*&where=1%3D1&f=geojson"
          )
    
    caltrans_districts = gpd.read_file(URL)[["DISTRICT", "geometry"]]
    
    caltrans_districts = caltrans_districts.assign(
        district_name = caltrans_districts.DISTRICT.map(
            portfolio_utils.district_name_dict)
    ).rename(columns = {"DISTRICT": "district"})
    
    # Spatial join to Caltrans district
    gdf2 = gpd.sjoin(
        gdf, 
        caltrans_districts.to_crs(gdf.crs),
        how = "inner",
        predicate = "intersects"
    ).drop(columns = "index_right")
    
    return gdf2
    
    
        gdf_with_district = spatial_join_to_caltrans_districts(gdf)
