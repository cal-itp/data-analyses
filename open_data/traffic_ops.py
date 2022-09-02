# Traffic Ops routes and stops data dictionary
KEYWORDS = [
    'Transportation',
    'GTFS',
    'Transit routes',
    'Transit stops',
    'Transit',
]


PURPOSE = "Provide all CA transit stops and routes (geospatial) from all transit operators."

METHODOLOGY = "This data was assembled from the General Transit Feed Specification (GTFS) schedule data. GTFS tables are text files, but these have been compiled for all operators and transformed into geospatial data, with minimal data processing. The transit routes dataset is assembled from two tables: (1) `shapes.txt`, which defines the route alignment path, and (2) `trips.txt` and `stops.txt`, for routes not found in `shapes.txt`. `shapes.txt` is an optional GTFS table with richer information than just transit stop longitude and latitude. For the routes that aren't found in `shapes.txt`, we compile the stop sequences with stop longitude/latitude to roughly capture the route alignment. The transit stops dataset is assembled from `stops.txt`, which contains information about the route, stop sequence, and stop longitude and latitude. References: https://gtfs.org/. https://gtfs.org/schedule/reference/#shapestxt. https://gtfs.org/schedule/reference/#stopstxt. https://gtfs.org/schedule/reference/#tripstxt."

ROUTES_DICT = {
    "dataset_name": "ca_transit_routes", 
    "publish_entity": "California Integrated Travel Project", 

    "abstract": "Public. EPSG: 4326",
    "purpose": PURPOSE, 

    "beginning_date": "20220713",
    "end_date": "20220813",
    "place": "California",

    "status": "Complete", 
    "frequency": "Monthly",
    
    "theme_keywords": KEYWORDS, 

    "methodology": METHODOLOGY, 
    
    "data_dict_type": "CSV",
    "data_dict_url": "some_url", 

    "contact_organization": "Caltrans", 
    "contact_person": "Tiffany Chu", 
    "contact_email": "tiffany.chu@dot.ca.gov",
    
    "horiz_accuracy": "0.00004 decimal degrees",
}

# Use same data dictionary with tiny modifications
STOPS_DICT = ROUTES_DICT.copy()
STOPS_DICT["dataset_name"] = "ca_transit_stops"
