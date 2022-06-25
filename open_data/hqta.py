# HQTA transit areas and transit stops data dictionary
KEYWORDS = [
    'Transportation',
    'Land Use',
    'Transit-Oriented Development',
    'TOD',
    'High Quality Transit'
]

PURPOSE = ('''
    Estimated High Quality Transit Areas as described in 
    Public Resources Code 21155, 21064.3, 21060.2.
    '''
)

METHODOLOGY = ('''
    This data was estimated using a spatial process derived from General Transit Feed Specification (GTFS) schedule data. To find high-quality bus corridors, we split each corridor into 1,500 meter segments and counted frequencies at the stop within that segment with the highest number of transit trips. If that stop saw at least 4 trips per hour for at least one hour in the morning, and again for at least one hour in the afternoon, we consider that segment a high-quality bus corridor. Segments without a stop are not considered high-quality corridors. Major transit stops were identified as either the intersection of two high-quality corridors from the previous step, a rail or bus rapid transit station, or a ferry terminal with bus service. Note that the definition of “bus rapid transit” in Public Resources Code 21060.2 includes features not captured by available data sources, these features were captured manually using information from transit agency sources and imagery. We believe this data to be broadly accurate, and fit for purposes including overall dashboards, locating facilities in relation to high quality transit areas, and assessing community transit coverage. However, the spatial determination of high-quality transit areas from GTFS data necessarily involves some assumptions as described above. Any critical determinations of whether a specific parcel is located within a high-quality transit area should be made in conjunction with local sources, such as transit agency timetables.  
    
    Notes: Null values may be present. The "hqta_details" columns defines which part of the Public Resources Code definition the HQTA classification was based on. If "hqta_details" references a single operator, then "itp_id_secondary" and "agency_secondary" are null. If "hqta_details" references the same operator, then "itp_id_secondary" and "agency_secondary" are the same as "itp_id_primary" and "agency_primary". 
    '''
)

HQTA_TRANSIT_AREAS_DICT = {
    "dataset_name": "ca_hq_transit_areas", 
    "publish_entity": "California Integrated Travel Project", 

    "abstract": "Public. EPSG: 4326",
    "purpose": PURPOSE, 

    "beginning_date": "20220624",
    "end_date": "20220724",
    "place": "California",

    "status": "Complete", 
    "frequency": "Monthly",
    
    "theme_keywords": KEYWORDS, 

    "methodology": METHODOLOGY, 
    
    "data_dict_type": "CSV",
    "data_dict_url": "some_url", 

    "contact_organization": "Caltrans", 
    "contact_person": "Eric Dasmalchi", 
    "contact_email": "eric.dasmalchi@dot.ca.gov" 

}

# Use same data dictionary with tiny modifications
HQTA_TRANSIT_STOPS_DICT = HQTA_TRANSIT_AREAS_DICT.copy()
HQTA_TRANSIT_STOPS_DICT["dataset_name"] = "ca_hq_transit_stops"