"""
Combine all the points for HQ transit open data portal.

From combine_and_visualize.ipynb
"""
import dask.dataframe as dd
import dask_geopandas as dg
import datetime as dt
import geopandas as gpd
import numpy as np
import os
import pandas as pd

import A3_rail_ferry_brt_extract as rail_ferry_brt_extract
import utilities
from shared_utils import utils, geography_utils, portfolio_utils
from update_vars import analysis_date

# Input files
MAJOR_STOP_BUS_FILE = utilities.catalog_filepath("major_stop_bus")
STOPS_IN_CORRIDOR_FILE = utilities.catalog_filepath("stops_in_hq_corr")
    
    
def get_agency_names() -> pd.DataFrame:
    names = portfolio_utils.add_agency_name(analysis_date)
    
    names = (names.astype({"calitp_itp_id": int})
             .rename(columns = {
                 "calitp_itp_id": "calitp_itp_id_primary", 
                 "calitp_agency_name": "agency_name_primary"})
            )
    
    return names


def add_agency_names_hqta_details(gdf: dg.GeoDataFrame) -> gpd.GeoDataFrame:
    """
    Add agency names by merging it in with our crosswalk
    to get the primary ITP ID and primary agency name.
    
    Then use a function to add secondary ITP ID and secondary agency name 
    and hqta_details column.
    hqta_details makes it clearer for open data portal users why
    some ID / agency name columns show the same info or are missing.
    """
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


def clean_up_hqta_points(gdf: gpd.GeoDataFrame) -> gpd.GeoDataFrame:
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
    major_stop_bus = dg.read_parquet(MAJOR_STOP_BUS_FILE)
    stops_in_corridor = dg.read_parquet(STOPS_IN_CORRIDOR_FILE)
    
    # Combine all the points data
    hqta_points_combined = (dd.multi.concat([major_stop_bus,
                                            stops_in_corridor,
                                            rail_ferry_brt,
                                           ], axis=0)
                            .astype({"calitp_itp_id_primary": int})
                           )
    
    time1 = dt.datetime.now()
    print(f"combined points: {time1 - start}")
    
    # Add agency names, hqta_details, project back to WGS84
    gdf = add_agency_names_hqta_details(hqta_points_combined)
    gdf = clean_up_hqta_points(gdf)
    
    time2 = dt.datetime.now()
    print(f"add agency names / compute: {time2 - time1}")
    
    utils.geoparquet_gcs_export(gdf,
                    utilities.GCS_FILE_PATH,
                    'hqta_points'
                   )    
    
    delete_local_files()
    
    # TODO: add export to individual folder as geojsonL
    # maybe create object loader fs.put in shared_utils
    # fs.mkdir(f'{GCS_FILE_PATH}export/{analysis_date.isoformat()}/')
    
    end = dt.datetime.now()
    print(f"execution time: {end-start}")
