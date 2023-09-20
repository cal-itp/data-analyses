#https://github.com/openaddresses/pyesridump

#pip install esridump
import geopandas as gpd
from calitp_data_analysis import get_fs
from shared_utils import utils

fs = get_fs()
GCS_FILE_PATH = "gs://calitp-analytics-data/data-analyses/shared_data/"

# In terminal:
'''
esri2geojson https://geo.dot.gov/server/rest/services/Hosted/California_2018_PR/FeatureServer/0 ca_roads.geojson
'''

if __name__ == "__main__":
    gdf = gpd.read_file("./ca_roads.geojson")
    
    
    utils.geojson_gcs_export(
        gdf,
        gcs_file_path = GCS_FILE_PATH,
        file_name =  "caltrans_all_roads_lrs",
        geojson_type = "geojson",
    )
    
    utils.geoparquet_gcs_export(
        gdf,
        GCS_FILE_PATH,
        "caltrans_all_roads_lrs"
    )