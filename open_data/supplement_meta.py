"""
Add to metadata.yml with longer descriptions and/or
sections that rely on functions.
"""
import json
import yaml

from pathlib import Path

from calitp_data_analysis import utils
from update_vars import analysis_date, ESRI_BASE_URL
from publish_utils import RENAME_HQTA, RENAME_SPEED

def get_esri_url(name: str)-> str:
    return f"{ESRI_BASE_URL}{name}/FeatureServer"

#--------------------------------------------------------#
# Define methodology
#--------------------------------------------------------#
HQTA_METHODOLOGY = "This data was estimated using a spatial process derived from General Transit Feed Specification (GTFS) schedule data. To find high-quality bus corridors, we split each corridor into 1,500 meter segments and counted frequencies at the stop within that segment with the highest number of transit trips. If that stop saw at least 4 trips per hour for at least one hour in the morning, and again for at least one hour in the afternoon, we consider that segment a high-quality bus corridor. Segments without a stop are not considered high-quality corridors. Major transit stops were identified as either the intersection of two high-quality corridors from the previous step, a rail or bus rapid transit station, or a ferry terminal with bus service. Note that the definition of `bus rapid transit` in Public Resources Code 21060.2 includes features not captured by available data sources, these features were captured manually using information from transit agency sources and imagery. We believe this data to be broadly accurate, and fit for purposes including overall dashboards, locating facilities in relation to high quality transit areas, and assessing community transit coverage. However, the spatial determination of high-quality transit areas from GTFS data necessarily involves some assumptions as described above. Any critical determinations of whether a specific parcel is located within a high-quality transit area should be made in conjunction with local sources, such as transit agency timetables.  Notes: Null values may be present. The `hqta_details` columns defines which part of the Public Resources Code definition the HQTA classification was based on. If `hqta_details` references a single operator, then `agency_secondary` and `base64_url_secondary` are null. If `hqta_details` references the same operator, then `agency_secondary` and `base64_url_secondary` are the same as `agency_primary` and `base64_url_primary`."

TRAFFIC_OPS_METHODOLOGY = "This data was assembled from the General Transit Feed Specification (GTFS) schedule data. GTFS tables are text files, but these have been compiled for all operators and transformed into geospatial data, with minimal data processing. The transit routes dataset is assembled from two tables: (1) `shapes.txt`, which defines the route alignment path, and (2) `trips.txt` and `stops.txt`, for routes not found in `shapes.txt`. `shapes.txt` is an optional GTFS table with richer information than just transit stop longitude and latitude. The transit stops dataset is assembled from `stops.txt`, which contains information about the route, stop sequence, and stop longitude and latitude. References: https://gtfs.org/. https://gtfs.org/schedule/reference/#shapestxt. https://gtfs.org/schedule/reference/#stopstxt. https://gtfs.org/schedule/reference/#tripstxt."

SEGMENT_METHODOLOGY = "This data was estimated by combining GTFS real-time vehicle positions to GTFS scheduled trips, shapes, stops, and stop times tables. GTFS shapes provides the route alignment path. Multiple trips may share the same shape, with a route typically associated with multiple shapes. Shapes are cut into segments at stop positions (stop_id-stop_sequence combination). A `stop segment` refers to the portion of shapes between the prior stop and the current stop. Vehicle positions are spatially joined to 35 meter buffered segments. Within each segment-trip, the first and last vehicle position observed are used to calculate the speed. Since multiple trips may occur over a segment each day, the multiple trip speeds provide a distribution. From this distribution, the 20th percentile, 50th percentile (median), and 80th percentile speeds are calculated. For all day speed metrics, all trips are used. For peak speed metrics, only trips with start times between 7 - 9:59 AM and 4 - 7:59 PM are used to find the 20th, 50th, and 80th percentile metrics. Data processing notes: (a) GTFS RT trips whose vehicle position timestamps span 10 minutes or less are dropped. Incomplete data would lead to unreliable estimates of speed at the granularity we need. (b) Segment-trip speeds of over 70 mph are excluded. These are erroneously calculated as transit does not typically reach those speeds. (c) Other missing or erroneous calculations, either arising from only one vehicle position found in a segment (change in time or change in distance cannot be calculated)."

ROUTE_METHODOLOGY = "This data was estimated by combining GTFS real-time vehicle positions with GTFS scheduled trips and shapes. GTFS real-time (RT) vehicle positions are spatially joined to GTFS scheduled shapes, so only vehicle positions traveling along the route alignment path are kept. A sample of five vehicle positions are selected (min, 25th percentile, 50th percentile, 75th percentile, max). The trip speed is calculated using these five vehicle positions. Each trip is categorized into a time-of-day. The average speed for a route-direction-time_of_day is calculated. Additional metrics are stored, such as the number of trips observed, the average scheduled service minutes, and the average RT observed service minutes. For convenience, we also provide a singular shape (common_shape_id) to associate with a route-direction. This is the shape that had the most number of trips for a given route-direction. Time-of-day is determined by the GTFS scheduled trip start time. The trip start hour (military time) is categorized based on the following: Owl (0-3), Early AM (4-6), AM Peak (7-9), Midday (10-14), PM Peak (15-19), and Evening (20-23). The start and end hours are inclusive (e.g., 4-6 refers to 4am, 5am, and 6am)."


#--------------------------------------------------------#
# Put supplemental parts together into dict
#--------------------------------------------------------#
supplement_me = {
    "ca_hq_transit_areas": {
        "methodology": HQTA_METHODOLOGY,
        "data_dict_url": get_esri_url("CA_HQ_Transit_Areas"),
        "revision_date": analysis_date,
        "rename_cols": RENAME_HQTA,
    },
    "ca_hq_transit_stops": {
        "methodology": HQTA_METHODOLOGY,
        "data_dict_url": get_esri_url("CA_HQ_Transit_Stops"), 
        "revision_date": analysis_date,
        "rename_cols": RENAME_HQTA,
    },
    "ca_transit_routes": {
        "methodology": TRAFFIC_OPS_METHODOLOGY,
        "data_dict_url": get_esri_url("CA_Transit_Routes"),
        "revision_date": analysis_date,
    },
    "ca_transit_stops": {
        "methodology": TRAFFIC_OPS_METHODOLOGY,
        "data_dict_url": get_esri_url("CA_Transit_Stops"),
        "revision_date": analysis_date,
    },
    "speeds_by_stop_segments": {
        "methodology": SEGMENT_METHODOLOGY,
        "data_dict_url": get_esri_url("Speeds_By_Stop_Segments"),
        "revision_date": analysis_date,
        "rename_cols": RENAME_SPEED,
    },
    "speeds_by_route_time_of_day": {
        "methodology": ROUTE_METHODOLOGY,
        "data_dict_url": get_esri_url("Speeds_By_Route_Time_of_Day"),
        "revision_date": analysis_date,
        "rename_cols": RENAME_SPEED,
    }
}


if __name__ == "__main__":
    
    METADATA_FILE = "metadata.yml"
    
    with open(METADATA_FILE) as f:
        meta = yaml.load(f, yaml.Loader)
        
    # The dictionaries for each dataset are stored in a list
    # Create a new list once we expand the dict
    new_table_list = []
    
    for i in meta["tables"]:
        # for each dataset, grab the relevant portion of 
        # the supplemental dict 
        table_name = i["dataset_name"]
        table_supplement = supplement_me[table_name]
        
        # Unpack the original dict and the supplemental portion into 1 dict
        expanded_dict = {**i, **table_supplement}
        
        new_table_list.append(expanded_dict)
    
    meta["tables"] = new_table_list
    
    # Format output as dict, where key is the dataset_name, not 0, 1, 2
    output = {i["dataset_name"]:i for i in meta["tables"]}
    
    # We lose formatting in yaml, especially in common-fields
    # Output a json to use in ArcPro, and only of the subset of dict that's meta["tables"]
    JSON_FILE = utils.sanitize_file_path(METADATA_FILE)
    
    with open(f"{JSON_FILE}.json", 'w') as f:
        json.dump(output, f)
    
    print(f"{JSON_FILE} produced")