"""
Combine all the points for HQ transit open data portal.

From combine_and_visualize.ipynb
"""
import dask.dataframe as dd
import dask_geopandas
import datetime as dt
import geopandas as gpd
import numpy as np
import os
import pandas as pd

from calitp.tables import tbl
from siuba import *

import A3_rail_ferry_brt_extract as rail_ferry_brt_extract
import utilities
from A1_rail_ferry_brt import analysis_date
from B1_bus_corridors import TEST_GCS_FILE_PATH
from shared_utils import utils, geography_utils

# Input files
MAJOR_STOP_BUS_FILE = utilities.catalog_filepath("major_stop_bus")
STOPS_IN_CORRIDOR_FILE = utilities.catalog_filepath("stops_in_hq_corr")
    
    
def get_agency_names():
    names = (tbl.views.gtfs_schedule_dim_feeds()
             >> filter(_.calitp_extracted_at < analysis_date, 
                       _.calitp_deleted_at > analysis_date)
             >> select(_.calitp_itp_id_primary == _.calitp_itp_id, 
                       _.agency_name_primary == _.calitp_agency_name)
             >> collect()
            )
    
    return names


def add_agency_names_hqta_details(gdf):
    names_df = get_agency_names()
    
    name_dict = (names_df.set_index('calitp_itp_id_primary')
                 .to_dict()['agency_name_primary']
                )
    
    gdf2 = dd.merge(gdf, 
                    names_df, 
                    on = "calitp_itp_id_primary",
                    how = "inner"
                   )
    
    with_names = gdf2.compute()
    
    with_names = with_names.assign(
                    agency_name_secondary = with_names.apply(
                        lambda x: name_dict[int(x.calitp_itp_id_secondary)] if 
                        (not np.isnan(x.calitp_itp_id_secondary) and 
                         int(x.calitp_itp_id_secondary) in name_dict.keys()) 
                         else None, axis = 1, 
                     ), 
                    hqta_details = with_names.apply(utilities.hqta_details, axis=1)
                )
    return with_names


def clean_up_hqta_points(gdf):
    gdf2 = (gdf.drop_duplicates(
                    subset=["calitp_itp_id_primary", "hqta_type", "stop_id"])
                   .sort_values(["calitp_itp_id_primary", "hqta_type", "stop_id"])
                   .reset_index(drop=True)
                   .to_crs(geography_utils.WGS84)
    )
    
    return gdf2


def delete_local_files():
    os.remove(MAJOR_STOP_BUS_FILE)
    os.remove(STOPS_IN_CORRIDOR_FILE)
    
    
if __name__=="__main__":
    start = dt.datetime.now()
    
    rail_ferry_brt = rail_ferry_brt_extract.get_rail_ferry_brt_extract()
    major_stop_bus = dask_geopandas.read_parquet(MAJOR_STOP_BUS_FILE)
    stops_in_corridor = dask_geopandas.read_parquet(STOPS_IN_CORRIDOR_FILE)
    
    # Combine all the points data
    hqta_points_combined = dd.multi.concat([major_stop_bus,
                                            stops_in_corridor,
                                            rail_ferry_brt,
                                           ], axis=0)
    
    
    time1 = dt.datetime.now()
    print(f"combined points: {time1 - start}")
    
    # Add agency names, hqta_details, project back to WGS84
    gdf = add_agency_names_hqta_details(hqta_points_combined)
    gdf = clean_up_hqta_points(gdf)
    
    time2 = dt.datetime.now()
    print(f"add agency names / compute: {time2 - time1}")
    
    utils.geoparquet_gcs_export(gdf,
                    f'{TEST_GCS_FILE_PATH}',
                    'hqta_points'
                   )    
    
    delete_local_files()
    
    # TODO: add export to individual folder as geojsonL
    # maybe create object loader fs.put in shared_utils
    # fs.mkdir(f'{GCS_FILE_PATH}export/{analysis_date.isoformat()}/')
    
    end = dt.datetime.now()
    print(f"execution time: {end-start}")
