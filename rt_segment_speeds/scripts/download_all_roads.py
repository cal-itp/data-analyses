#https://github.com/openaddresses/pyesridump

#pip install esridump
import geopandas as gpd
from calitp_data_infra.storage import get_fs 
from calitp_data_analysis import utils
from update_vars import SHARED_GCS

fs = get_fs()

# In terminal:
'''
esri2geojson https://geo.dot.gov/server/rest/services/Hosted/California_2018_PR/FeatureServer/0 ca_roads.geojson
'''

if __name__ == "__main__":
    gdf = gpd.read_file("./ca_roads.geojson")
    
    
    utils.geojson_gcs_export(
        gdf,
        gcs_file_path = SHARED_GCS,
        file_name =  "caltrans_all_roads_lrs",
        geojson_type = "geojson",
    )
    
    utils.geoparquet_gcs_export(
        gdf,
        SHARED_GCS,
        "caltrans_all_roads_lrs"
    )