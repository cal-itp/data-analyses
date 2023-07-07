from gcs_to_esri import open_data_dates
from metadata_hqta import ESRI_BASE_URL

# Traffic Ops routes and stops data dictionary
KEYWORDS = [
    'Transportation',
    'GTFS',
    'Transit routes',
    'Transit stops',
    'Transit',
]


PURPOSE = "Provide all CA transit stops and routes (geospatial) from all transit operators."

METHODOLOGY = "This data was assembled from the General Transit Feed Specification (GTFS) schedule data. GTFS tables are text files, but these have been compiled for all operators and transformed into geospatial data, with minimal data processing. The transit routes dataset is assembled from two tables: (1) `shapes.txt`, which defines the route alignment path, and (2) `trips.txt` and `stops.txt`, for routes not found in `shapes.txt`. `shapes.txt` is an optional GTFS table with richer information than just transit stop longitude and latitude. The transit stops dataset is assembled from `stops.txt`, which contains information about the route, stop sequence, and stop longitude and latitude. References: https://gtfs.org/. https://gtfs.org/schedule/reference/#shapestxt. https://gtfs.org/schedule/reference/#stopstxt. https://gtfs.org/schedule/reference/#tripstxt."

DATA_DICT_ROUTES_URL = f"{ESRI_BASE_URL}CA_Transit_Routes/FeatureServer"
DATA_DICT_STOPS_URL = f"{ESRI_BASE_URL}CA_Transit_Stops/FeatureServer"

ROUTES_DICT = {
    "dataset_name": "ca_transit_routes", 
    "publish_entity": "California Integrated Travel Project", 

    "abstract": "Public. EPSG: 4326",
    "purpose": PURPOSE, 

    "creation_date": "2022-02-08",
    "beginning_date": open_data_dates()[0],
    "end_date": open_data_dates()[1],
    "place": "California",

    "status": "completed", 
    "frequency": "monthly",
    
    "theme_keywords": KEYWORDS, #tags

    "methodology": METHODOLOGY, 
    
    "data_dict_type": "CSV",
    "data_dict_url": DATA_DICT_ROUTES_URL, 

    "contact_organization": "Caltrans", 
    "contact_person": "Tiffany Ku", 
    "contact_email": "tiffany.ku@dot.ca.gov",
    
    "horiz_accuracy": "4 meters",
}

# Use same data dictionary with tiny modifications
STOPS_DICT = ROUTES_DICT.copy()
STOPS_DICT["dataset_name"] = "ca_transit_stops"
STOPS_DICT["data_dict_url"] = DATA_DICT_STOPS_URL