from gcs_to_esri import open_data_dates

# HQTA transit areas and transit stops data dictionary
KEYWORDS = [
    'Transportation',
    'Land Use',
    'Transit-Oriented Development',
    'TOD',
    'High Quality Transit'
]

PURPOSE_AREAS = ('''
    Estimated High Quality Transit Areas as described in Public Resources Code 21155, 21064.3, 21060.2.
    '''
)

PURPOSE_STOPS = ('''
    Estimated stops along High Quality Transit Corridors, plus major transit stops for bus rapid transit, ferry, rail modes as described in Public Resources Code 21155, 21064.3, 21060.2
    '''
)

METHODOLOGY = "This data was estimated using a spatial process derived from General Transit Feed Specification (GTFS) schedule data. To find high-quality bus corridors, we split each corridor into 1,500 meter segments and counted frequencies at the stop within that segment with the highest number of transit trips. If that stop saw at least 4 trips per hour for at least one hour in the morning, and again for at least one hour in the afternoon, we consider that segment a high-quality bus corridor. Segments without a stop are not considered high-quality corridors. Major transit stops were identified as either the intersection of two high-quality corridors from the previous step, a rail or bus rapid transit station, or a ferry terminal with bus service. Note that the definition of `bus rapid transit` in Public Resources Code 21060.2 includes features not captured by available data sources, these features were captured manually using information from transit agency sources and imagery. We believe this data to be broadly accurate, and fit for purposes including overall dashboards, locating facilities in relation to high quality transit areas, and assessing community transit coverage. However, the spatial determination of high-quality transit areas from GTFS data necessarily involves some assumptions as described above. Any critical determinations of whether a specific parcel is located within a high-quality transit area should be made in conjunction with local sources, such as transit agency timetables.  Notes: Null values may be present. The `hqta_details` columns defines which part of the Public Resources Code definition the HQTA classification was based on. If `hqta_details` references a single operator, then `agency_secondary` and `base64_url_secondary` are null. If `hqta_details` references the same operator, then `agency_secondary` and `base64_url_secondary` are the same as `agency_primary` and `base64_url_primary`. Refer to https://github.com/cal-itp/data-analyses/blob/main/high_quality_transit_areas/README.md for more details."


ESRI_BASE_URL = "https://gisdata.dot.ca.gov/arcgis/rest/services/CHrailroad/"
DATA_DICT_AREAS_URL = f"{ESRI_BASE_URL}CA_HQ_Transit_Areas/FeatureServer"
DATA_DICT_STOPS_URL = f"{ESRI_BASE_URL}CA_HQ_Transit_Stops/FeatureServer"

HQTA_TRANSIT_AREAS_DICT = {
    "dataset_name": "ca_hq_transit_areas", 
    "publish_entity": "Data & Digital Services / California Integrated Travel Project", 

    "abstract": "Public. EPSG: 4326",
    "purpose": PURPOSE_AREAS, 

    "creation_date": "2022-02-08",
    "beginning_date": open_data_dates()[0],
    "end_date": open_data_dates()[1],
    "place": "California",

    "status": "completed", 
    "frequency": "monthly",
    
    "theme_keywords": KEYWORDS, 

    "methodology": METHODOLOGY, 
    
    "data_dict_type": "CSV",
    "data_dict_url": DATA_DICT_AREAS_URL, 

    "contact_organization": "Caltrans", 
    "contact_person": "Eric Dasmalchi", 
    "contact_email": "eric.dasmalchi@dot.ca.gov", 
    
    "horiz_accuracy": "4 meters",
}

# Use same data dictionary with tiny modifications
HQTA_TRANSIT_STOPS_DICT = HQTA_TRANSIT_AREAS_DICT.copy()
HQTA_TRANSIT_STOPS_DICT["dataset_name"] = "ca_hq_transit_stops"
HQTA_TRANSIT_STOPS_DICT["purpose"] = PURPOSE_AREAS
HQTA_TRANSIT_STOPS_DICT["data_dict_url"] = DATA_DICT_STOPS_URL

# Rename columns
RENAME_CA_HQTA = {
    "agency_pri": "agency_primary",
    "agency_sec": "agency_secondary",
    "hqta_detai": "hqta_details",
    "base64_url": "base64_url_primary",
    "base64_u_1": "base64_url_secondary",  
    "org_pri": "org_id_primary",
    "org_sec": "org_id_secondary",
}