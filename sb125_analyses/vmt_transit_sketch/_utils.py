import pygris
import geopandas as gpd
from siuba import *
from calitp_data_analysis.geography_utils import CA_NAD83Albers

GCS_PATH = 'gs://calitp-analytics-data/data-analyses/sb125/vmt_transit_sketch/'

def get_tract_geoms():
    ca_tracts = pygris.tracts(state='CA', year=2020)
    ca_tracts = ca_tracts >> select(_.GEOID, _.geometry)
    ca_tracts = ca_tracts.to_crs(CA_NAD83Albers)
    
    return ca_tracts