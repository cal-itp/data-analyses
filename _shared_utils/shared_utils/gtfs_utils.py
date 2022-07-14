"""
GTFS utils

TODO: move over some of the rt_utils
over here, if it addresses GTFS schedule data more generally, 
such as cleaning/reformatting arrival times.

Leave the RT-specific analysis there.
"""

import pandas as pd


def map_metrolink_substring(s, my_dict):
    for key, value in my_dict.items():
        if key in s:
            return my_dict[key]
        

METROLINK_SHAPE_TO_ROUTE = {
    'SB': 'San Bernardino Line', 
    'IE': 'Inland Emp.-Orange Co. Line',
    'OCin': 'Orange County Line',  
    'OCout': 'Orange County Line',
    'RIVER': 'Riverside Line', 
    'AV': 'Antelope Valley Line', 
    'VT': 'Ventura County Line',
    'LAX': 'LAX FlyAway Bus', 
    '91': '91 Line',
}  

METROLINK_SHAPE_TO_ROUTE = {
    "AVin": "Antelope Valley Line",
    "AVout": "Antelope Valley Line",
    "OCin": "Orange County Line",
    "OCout": "Orange County Line",
    "LAXin": "LAX FlyAway Bus",
    "LAXout": "LAX FlyAway Bus",
    "SBin": "San Bernardino Line", 
    "SBout": "San Bernardino Line",
    "VTin": "Ventura County Line",
    "VTout": "Ventura County Line",
    "91in": "91 Line",
    "91out": "91 Line", 
    "IEOCin": "Inland Emp.-Orange Co. Line", 
    "IEOCout": "Inland Emp.-Orange Co. Line",
    "RIVERin": "Riverside Line",
    "RIVERout": "Riverside Line",
}

    
def fill_in_metrolink_trips_df_with_shape_id(trips):
    """
    trips: pandas.DataFrame. 
            What is returned from tbl.views.gtfs_schedule_dim_trips()
            or some dataframe derived from that.
            
    Returns only Metrolink rows, with shape_id filled in.
    """
    # Even if an entire routes df is supplied, subset to just Metrolink
    df = trips[trips.calitp_itp_id==323].reset_index(drop=True)
    
    # direction_id==1 (inbound), toward LA Union Station
    # direction_id==0 (outbound), toward Irvine/Oceanside, etc
    
    df = df.assign(
        shape_id = df.route_id.apply(lambda x: METROLINK_ROUTE_TO_SHAPE[x])
    )
    
    # OCin and OCout are not distinguished in dictionary
    df = df.assign(
        shape_id = df.apply(lambda x: "OCin" if x.direction_id=="0", axis=1)
    )
    
    return df