# Traffic Ops routes and stops data dictionary
from metadata_update import fill_in_keyword_list

KEYWORDS = [
    'Transportation',
    'GTFS',
    'Transit routes',
    'Transit stops',
    'Transit',
]

KEYWORDS_FORMATTED = fill_in_keyword_list(
    topic='transportation', keyword_list = KEYWORDS)


PURPOSE = ('''
    Purpose.
    '''
)

METHODOLOGY = ('''
    Methodology.
    '''
)

ROUTES_DICT = {
    "dataset_name": "ca_transit_routes", 
    "publish_entity": "California Integrated Travel Project", 

    "abstract": "Public. EPSG: 3310",
    "purpose": PURPOSE, 

    "beginning_date": "20220608",
    "end_date": "20220708",
    "place": "California",

    "status": "Complete", 
    "frequency": "Monthly",
    
    "theme_topics": KEYWORDS_FORMATTED, 

    "methodology": METHODOLOGY, 
    
    "data_dict_type": "CSV",
    "data_dict_url": "some_url", 

    "contact_organization": "Caltrans", 
    "contact_person": "Tiffany Chu", 
    "contact_email": "tiffany.chu@dot.ca.gov" 
}

# Use same data dictionary with tiny modifications
STOPS_DICT = ROUTES_DICT.copy()
STOPS_DICT["dataset_name"] = "ca_transit_stops"
