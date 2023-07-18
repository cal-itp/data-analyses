"""
Speeds datasets metadata elements
"""
from gcs_to_esri import open_data_dates
from update_vars import ESRI_BASE_URL

KEYWORDS = [
    'Transportation',
    'Transit',
    'GTFS',
    'GTFS RT',
    'real time',
    'speeds', 
    'vehicle positions'
]


SEGMENT_PURPOSE = "All day and peak transit speeds by segments for all CA operators that provide GTFS real-time vehicle positions data."

SEGMENT_ABSTRACT = "All day and peak transit 20th, 50th, and 80th percentile speeds on stop segments estimated on a single day for all CA transit operators that provide GTFS real-time vehicle positions data."

SEGMENT_METHODOLOGY = "This data was estimated by combining GTFS real-time vehicle positions to GTFS scheduled trips, shapes, stops, and stop times tables. GTFS shapes provides the route alignment path. Multiple trips may share the same shape, with a route typically associated with multiple shapes. Shapes are cut into segments at stop positions (stop_id-stop_sequence combination). A `stop segment` refers to the portion of shapes between the prior stop and the current stop. Vehicle positions are spatially joined to 35 meter buffered segments. Within each segment-trip, the first and last vehicle position observed are used to calculate the speed. Since multiple trips may occur over a segment each day, the multiple trip speeds provide a distribution. From this distribution, the 20th percentile, 50th percentile (median), and 80th percentile speeds are calculated. For all day speed metrics, all trips are used. For peak speed metrics, only trips with start times between 7 - 9:59 AM and 4 - 7:59 PM are used to find the 20th, 50th, and 80th percentile metrics. Data processing notes: (a) GTFS RT trips whose vehicle position timestamps span 10 minutes or less are dropped. Incomplete data would lead to unreliable estimates of speed at the granularity we need. (b) Segment-trip speeds of over 70 mph are excluded. These are erroneously calculated as transit does not typically reach those speeds. (c) Other missing or erroneous calculations, either arising from only one vehicle position found in a segment (change in time or change in distance cannot be calculated)."


ROUTE_PURPOSE = "Average transit speeds by route-direction and time-of-day estimated on a single day for all CA transit operators that provide GTFS real-time vehicle positions data."

ROUTE_ABSTRACT = "Provide average transit speeds, number of trips, and metrics related to scheduled annd real-time service minutes by route-direction and time-of-day."

ROUTE_METHODOLOGY = "This data was estimated by combining GTFS real-time vehicle positions with GTFS scheduled trips and shapes. GTFS real-time (RT) vehicle positions are spatially joined to GTFS scheduled shapes, so only vehicle positions traveling along the route alignment path are kept. A sample of five vehicle positions are selected (min, 25th percentile, 50th percentile, 75th percentile, max). The trip speed is calculated using these five vehicle positions. Each trip is categorized into a time-of-day. The average speed for a route-direction-time_of_day is calculated. Additional metrics are stored, such as the number of trips observed, the average scheduled service minutes, and the average RT observed service minutes. For convenience, we also provide a singular shape (common_shape_id) to associate with a route-direction. This is the shape that had the most number of trips for a given route-direction. Time-of-day is determined by the GTFS scheduled trip start time. The trip start hour (military time) is categorized based on the following: Owl (0-3), Early AM (4-6), AM Peak (7-9), Midday (10-14), PM Peak (15-19), and Evening (20-23). The start and end hours are inclusive (e.g., 4-6 refers to 4am, 5am, and 6am)."


DATA_DICT_SPEEDS_URL = f"{ESRI_BASE_URL}Speeds_By_Stop_Segments/FeatureServer"
DATA_DICT_ROUTE_SPEEDS_URL = f"{ESRI_BASE_URL}Speeds_By_Route_Time_of_Day/FeatureServer"

SPEEDS_STOP_SEG_DICT = {
    "dataset_name": "speeds_by_stop_segments", 
    "publish_entity": "Data & Digital Services / California Integrated Travel Project", 

    "purpose": SEGMENT_PURPOSE, 
    "abstract": SEGMENT_ABSTRACT,

    "creation_date": "2023-06-14",
    "beginning_date": open_data_dates()[0],
    "end_date": open_data_dates()[1],
    "place": "California",

    "status": "completed", 
    "frequency": "monthly",
    
    "theme_keywords": KEYWORDS, #tags

    "methodology": SEGMENT_METHODOLOGY, 
    
    "data_dict_type": "CSV",
    "data_dict_url": DATA_DICT_SPEEDS_URL, 

    "contact_organization": "Caltrans", 
    "contact_person": "Tiffany Ku; Eric Dasmalchi", 
    "contact_email": "tiffany.ku@dot.ca.gov; eric.dasmalchi@dot.ca.gov",
    
    "horiz_accuracy": "4 meters",
}


ROUTE_SPEEDS_DICT = {
    "dataset_name": "speeds_by_route_time_of_day", 
    "publish_entity": "Data & Digital Services / California Integrated Travel Project", 

    "purpose": ROUTE_PURPOSE, 
    "abstract": ROUTE_ABSTRACT,

    "creation_date": "2023-06-14",
    "beginning_date": open_data_dates()[0],
    "end_date": open_data_dates()[1],
    "place": "California",

    "status": "completed", 
    "frequency": "monthly",
    
    "theme_keywords": KEYWORDS, #tags

    "methodology": ROUTE_METHODOLOGY, 
    
    "data_dict_type": "CSV",
    "data_dict_url": DATA_DICT_ROUTE_SPEEDS_URL, 

    "contact_organization": "Caltrans", 
    "contact_person": "Tiffany Ku; Eric Dasmalchi", 
    "contact_email": "tiffany.ku@dot.ca.gov; eric.dasmalchi@dot.ca.gov",
    
    "horiz_accuracy": "4 meters",
}