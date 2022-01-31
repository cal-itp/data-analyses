"""
One-off functions, run once, save datasets for shared use.
"""
import geopandas as gpd
import pandas as pd


## Function to set the county centroids and zoom levels
# used in folium and ipyleaflet maps
def make_county_centroids():
    URL = "https://opendata.arcgis.com/datasets/8713ced9b78a4abb97dc130a691a8695_0.geojson"
    
    gdf = gpd.read_file(URL).to_crs(geography_utils.CA_StatePlane)  
    gdf.columns = gdf.columns.str.lower()
    
    gdf= (gdf.assign(area = gdf.geometry.area)
          # Sort in descending order
          # Ex: LA County, where there are separate polygons for Channel Islands
          # centroid should be based on where the biggest polygon is
          .sort_values(["county_name", "area"], ascending=[True, False])
          .drop_duplicates(subset="county_name")
          .reset_index()
          .to_crs(geography_utils.WGS84)
         )
    
    # Grab the centroid
    gdf = gdf.assign(
        longitude = (gdf.geometry.centroid.x).round(2),
        latitude = (gdf.geometry.centroid.y).round(2),
    )
    
    # Make the centroid into a list with [latitude, longitude] format
    gdf = gdf.assign(
        centroid = gdf.apply(lambda x: list( 
                [x.latitude, x.longitude]), axis=1),
        zoom = 11,
    )[["county_name", "centroid", "zoom"]]
    
    # Create statewide zoom parameters
    ca_row = {
        "county_name": "CA",
        "centroid": [35.8, -119.4], 
        "zoom": 6
    }
    
    # Do transpose to get it to display match how columns are displayed
    ca_row2 = pd.DataFrame.from_dict(ca_row, orient="index").T
    gdf2 = gdf.append(ca_row2).reset_index(drop=True)
    
    print("County centroids dataset created")
    
    # Save as parquet, because lat/lon held in list, not point geometry anymore
    gdf2.to_parquet("gs://calitp-analytics-data/data-analyses/ca_county_centroids.parquet")

    print("County centroids exported to GCS")


# Run functions to create these datasets...store in GCS
if __name__ == "__main__":
    # Don't use from shared_utils import geography_utils
    # Those have other dependencies...like map_utils imports from geography_utils
    import geography_utils
    import utils

    make_county_centroids()