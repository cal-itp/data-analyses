import datetime as dt
import geopandas as gpd
import numpy as np
import pandas as pd

from calitp.tables import tbl
from siuba import *

import prep_data
import shared_utils
import utils


def generate_shape_categories(shapes_df):
    shapes_df = (shapes_df.reset_index(drop=True)
                 .to_crs(shared_utils.geography_utils.CA_NAD83Albers)
                )
    
    shapes_df = shapes_df.assign(
        geometry = shapes_df.geometry.simplify(tolerance=1),
        route_length = shapes_df.geometry.length
    )
    
    ## quick fix for invalid geometries?
    ces_df = (prep_data.generate_calenviroscreen_lehd_data()
              .to_crs(shared_utils.geography_utils.CA_NAD83Albers)
             )

    ces_df = ces_df.assign(
        tract_type = ces_df['pop_sq_mi'].apply(lambda x: 'urban' if x > 2400 
                                               else 'suburban' if x > 800 
                                               else 'rural'),
        ## quick fix for invalid geometries (comes up in dissolve later)
        geometry = ces_df.geometry.buffer(0),
    )
    
    category_dissolved = ces_df[["tract_type", "geometry"]].dissolve(by='tract_type')
    # Since CRS is in meters, tolerance is anything < 1m off gets straightened
    category_dissolved["geometry"] = category_dissolved.geometry.simplify(tolerance=1)
    

    urban = shapes_df.clip(category_dissolved.loc[['urban']])
    suburban = shapes_df.clip(category_dissolved.loc[['suburban']])
    rural = shapes_df.clip(category_dissolved.loc[['rural']])

    shapes_df['pct_urban'] = urban.geometry.length / shapes_df.geometry.length
    shapes_df['pct_suburban'] = suburban.geometry.length / shapes_df.geometry.length
    shapes_df['pct_rural'] = rural.geometry.length / shapes_df.geometry.length
    
    shapes_df['pct_max'] = shapes_df[
        ['pct_urban', 'pct_suburban', 'pct_rural']].max(axis=1)
    
    return shapes_df


def categorize_shape(row):
    if row.pct_urban == row.pct_max:
        row['tract_type'] = 'urban'
    elif row.pct_suburban == row.pct_max:
        row['tract_type'] = 'suburban'
    elif row.pct_rural == row.pct_max:
        row['tract_type'] = 'rural'
    else:
        row['tract_type'] = np.nan
    return row


def create_shapes_tract_categorized():
    #DATA_PATH = "./data/test/"
    DATA_PATH = "gs://calitp-analytics-data/data-analyses/bus_service_increase/test/"
    
    time0 = dt.datetime.now()
    
    # Move creating linestring to this
    itp_ids = tbl.gtfs_schedule.agency() >> distinct(_.calitp_itp_id) >> collect()
    itp_ids = list(itp_ids.calitp_itp_id)    
    print(f"Grab ITP IDs")
    print(itp_ids)
    
    #all_shapes = shared_utils.geography_utils.make_routes_shapefile(ITP_ID_LIST = itp_ids)
    
    time1 = dt.datetime.now()
    print(f"Execution time to make routes shapefile: {time1-time0}")
    
    #all_shapes.to_parquet(f"{DATA_PATH}shapes_initial.parquet")
    #shared_utils.utils.geoparquet_gcs_export(all_shapes, utils.GCS_FILE_PATH, 'shapes_initial')
    
    #all_shapes = gpd.read_parquet(f'{utils.GCS_FILE_PATH}shapes_initial.parquet')
    all_shapes = gpd.read_parquet(f"{DATA_PATH}shapes_initial.parquet")
    
    time2 = dt.datetime.now()
    processed_shapes = generate_shape_categories(all_shapes)
    processed_shapes = processed_shapes.apply(categorize_shape, axis=1)
    
    # When should tract_type is NaN rows be dropped? Maybe right before analysis
    
    
    print(f"Execution time to categorize routes: {time2-time1}")
    processed_shapes.to_parquet(f"{DATA_PATH}shapes_processed.parquet")
    #shared_utils.utils.geoparquet_gcs_export(processed_shapes, utils.GCS_FILE_PATH, 
    #                                         'shapes_processed')
    
    print(f"Total execution time: {time2-time0}")
    
    
create_shapes_tract_categorized()