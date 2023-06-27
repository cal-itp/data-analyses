from gcs_to_esri import open_data_dates
from metadata_hqta import ESRI_BASE_URL

# Speeds data dictionary
KEYWORDS = [
    'Transportation',
    'Transit',
    'GTFS',
    'GTFS RT',
    'real time',
    'speeds', 
    'vehicle positions'
]


PURPOSE = "All day and peak transit speeds on stop segments estimated on a single day."

METHODOLOGY = (
    '''
    This data was estimated by combining GTFS real-time vehicle positions to GTFS scheduled trips, shapes, stops, and stop times tables. GTFS shapes provides the route alignment path, and multiple trips share the same shape. Shapes are cut into segments at stop positions. A `stop segment` refers to the portion of shapes between the prior stop and the current stop. Vehicle positions are spatially joined to segments, and speeds are calculated within each segment for each trip. Multiple trip speeds within each segment provide a distribution, from which 20th percentile, 50th percentile (median), and 80th percentile speeds can be calculated. For all day speed metrics, all trips are used. For peak speed metrics, only trips with start times between 7 - 9:59 AM and 4 - 7:59 PM are used.  
    '''
)

DATA_DICT_SPEEDS_URL = f"{ESRI_BASE_URL}CA_Transit_Routes/FeatureServer"

SPEEDS_STOP_SEG_DICT = {
    "dataset_name": "ca_speeds_stop_segments", 
    "publish_entity": "Data & Digital Services / California Integrated Travel Project", 

    "abstract": "Public. EPSG: 4326",
    "purpose": PURPOSE, 

    "beginning_date": open_data_dates()[0],
    "end_date": open_data_dates()[1],
    "place": "California",

    "status": "completed", 
    "frequency": "monthly",
    
    "theme_keywords": KEYWORDS, #tags

    "methodology": METHODOLOGY, 
    
    "data_dict_type": "CSV",
    "data_dict_url": "some_url", 

    "contact_organization": "Caltrans", 
    "contact_person": "Tiffany Ku; Eric Dasmalchi", 
    "contact_email": "tiffany.ku@dot.ca.gov; eric.dasmalchi@dot.ca.gov",
    
    "horiz_accuracy": "4 meters",
}
