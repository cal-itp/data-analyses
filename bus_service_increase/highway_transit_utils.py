"""
Utility functions for working with
State Highway Network and transit routes.
"""
import geopandas as gpd
import intake
import pandas as pd

import shared_utils
import utils

#--------------------------------------------------------#
### State Highway Network
#--------------------------------------------------------#
def clean_highways():
    HIGHWAY_URL = ("https://opendata.arcgis.com/datasets/"
                   "77f2d7ba94e040a78bfbe36feb6279da_0.geojson")
    gdf = gpd.read_file(HIGHWAY_URL)
    
    drop_cols = ['OBJECTID', 'RteSuffix',
                 'PMPrefix', 'bPM', 'ePM',
                 'PMSuffix', 'bPMc', 'ePMc',
                 'bOdometer', 'eOdometer', 
                 'AlignCode', 'Shape_Length',
                ]
    gdf = gdf.drop(columns=drop_cols)
    
    print(f"# rows before dissolve: {len(gdf)}")
    
    # See if we can dissolve further - use all cols except geometry
    # Should we dissolve further and use even longer lines?
    dissolve_cols = list(gdf.columns)
    dissolve_cols.remove('geometry')
    
    gdf2 = gdf.dissolve(by=dissolve_cols).reset_index()
    print(f"# rows after dissolve: {len(gdf2)}")
    
    return gdf2


state_highway_network = clean_highways()
shared_utils.utils.geoparquet_gcs_export(state_highway_network,
                                         utils.GCS_FILE_PATH, 
                                         "state_highway_network")
