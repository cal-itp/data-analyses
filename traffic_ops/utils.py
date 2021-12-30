"""
Utility functions from _shared_utils

Since Airflow runs in data-infra,
rather than install all the shared_utils, 
just lift the relevant functions needed for scripts.
"""
import fsspec
import geopandas as gpd
import os
import pandas as pd
import shapely
import shutil

os.environ["CALITP_BQ_MAX_BYTES"] = str(50_000_000_000)

import calitp
from calitp.tables import tbl
from calitp.storage import get_fs
from siuba import *
fs = get_fs()

#----------------------------------------------------------#
## _shared_utils/utils.py
# https://github.com/cal-itp/data-analyses/blob/main/_shared_utils/shared_utils/utils.py
#----------------------------------------------------------#
def geoparquet_gcs_export(gdf, GCS_FILE_PATH, FILE_NAME):
    '''
    Save geodataframe as parquet locally, 
    then move to GCS bucket and delete local file.
    
    gdf: geopandas.GeoDataFrame
    GCS_FILE_PATH: str. Ex: gs://calitp-analytics-data/data-analyses/my-folder/
    FILE_NAME: str. Filename.
    '''
    gdf.to_parquet(f"./{FILE_NAME}.parquet")
    fs.put(f"./{FILE_NAME}.parquet", f"{GCS_FILE_PATH}{FILE_NAME}.parquet")
    os.remove(f"./{FILE_NAME}.parquet")

    
def download_geoparquet(GCS_FILE_PATH, FILE_NAME, save_locally=False):
    """
    Parameters:
    GCS_FILE_PATH: str. Ex: gs://calitp-analytics-data/data-analyses/my-folder/
    FILE_NAME: str, name of file (without the .parquet).
                Ex: test_file (not test_file.parquet)
    save_locally: bool, defaults to False. if True, will save geoparquet locally.
    """
    object_path = fs.open(f"{GCS_FILE_PATH}{FILE_NAME}.parquet")
    gdf = gpd.read_parquet(object_path)
    
    if save_locally is True:
        gdf.to_parquet(f"./{FILE_NAME}.parquet")
    
    return gdf
    
    
# Make zipped shapefile
# https://github.com/CityOfLosAngeles/planning-entitlements/blob/master/notebooks/utils.py
def make_zipped_shapefile(df, path):
    """
    Make a zipped shapefile and save locally
    Parameters
    ==========
    df: gpd.GeoDataFrame to be saved as zipped shapefile
    path: str, local path to where the zipped shapefile is saved.
            Ex: "folder_name/census_tracts" 
                "folder_name/census_tracts.zip"
                
    Remember: ESRI only takes 10 character column names!!
    """
    # Grab first element of path (can input filename.zip or filename)
    dirname = os.path.splitext(path)[0]
    print(f"Path name: {path}")
    print(f"Dirname (1st element of path): {dirname}")
    # Make sure there's no folder with the same name
    shutil.rmtree(dirname, ignore_errors=True)
    # Make folder
    os.mkdir(dirname)
    shapefile_name = f"{os.path.basename(dirname)}.shp"
    print(f"Shapefile name: {shapefile_name}")
    # Export shapefile into its own folder with the same name
    df.to_file(driver="ESRI Shapefile", filename=f"{dirname}/{shapefile_name}")
    print(f"Shapefile component parts folder: {dirname}/{shapefile_name}")
    # Zip it up
    shutil.make_archive(dirname, "zip", dirname)
    # Remove the unzipped folder
    shutil.rmtree(dirname, ignore_errors=True)
    
    
#----------------------------------------------------------#
## _shared_utils/geography_utils.py
# https://github.com/cal-itp/data-analyses/blob/main/_shared_utils/shared_utils/geography_utils.py
#----------------------------------------------------------# 
WGS84 = "EPSG:4326"
CA_StatePlane = "EPSG:2229" # units are in feet
CA_NAD83Albers = "EPSG:3310" # units are in meters

SQ_MI_PER_SQ_M = 3.86 * 10**-7

# Function to take transit stop point data and create lines 
def make_routes_shapefile(ITP_ID_LIST = [], CRS="EPSG:4326", alternate_df=None):
    """
    Parameters:
    ITP_ID_LIST: list. List of ITP IDs found in agencies.yml
    CRS: str. Default is WGS84, but able to re-project to another CRS.
    
    Returns a geopandas.GeoDataFrame, where each line is the operator-route-line geometry.
    """
        
    all_routes = gpd.GeoDataFrame()
    
    for itp_id in ITP_ID_LIST:
        if alternate_df is None:
            shapes = (tbl.gtfs_schedule.shapes()
                      >> filter(_.calitp_itp_id == int(itp_id)) 
                      >> collect()
            )
        
        elif alternate_df is not None:
            shapes = alternate_df.copy()
            # shape_id is None, which will throw up an error later on when there's groupby
            shapes = shapes.assign(
                shape_id = shapes.route_id,
            )
        
        # Make a gdf
        shapes = (gpd.GeoDataFrame(shapes, 
                              geometry = gpd.points_from_xy
                              (shapes.shape_pt_lon, shapes.shape_pt_lat),
                              crs = WGS84)
             )
        
        # Count the number of stops for a given shape_id
        # Must be > 1 (need at least 2 points to make a line)
        shapes = shapes.assign(
            num_stops = (shapes.groupby("shape_id")["shape_pt_sequence"]
                         .transform("count")
                        )
        )
        
        # Drop the shape_ids that can't make a line
        shapes = shapes[shapes.num_stops > 1].reset_index(drop=True)
                
        # Now, combine all the stops by stop sequence, and create linestring
        for route in shapes.shape_id.unique():
            single_shape = (shapes
                            >> filter(_.shape_id == route)
                            >> mutate(shape_pt_sequence = _.shape_pt_sequence.astype(int))
                            # arrange in the order of stop sequence
                            >> arrange(_.shape_pt_sequence)
            )
            
            # Convert from a bunch of points to a line (for a route, there are multiple points)
            route_line = shapely.geometry.LineString(list(single_shape['geometry']))
            single_route = (single_shape
                           [['calitp_itp_id', 'shape_id']]
                           .iloc[[0]]
                          ) ##preserve info cols
            single_route['geometry'] = route_line
            single_route = gpd.GeoDataFrame(single_route, crs=WGS84)
            
            all_routes = all_routes.append(single_route)
    
    all_routes = (all_routes.to_crs(CRS)
                  .sort_values(["calitp_itp_id", "shape_id"])
                  .drop_duplicates()
                  .reset_index(drop=True)
                 )
    
    return all_routes


def create_point_geometry(df, longitude_col = "stop_lon", 
                         latitude_col = "stop_lat", crs = WGS84):
    """
    Parameters:
    df: pandas.DataFrame to turn into geopandas.GeoDataFrame, 
        default dataframe in mind is gtfs_schedule.stops
        
    longitude_col: str, column name corresponding to longitude
                    in gtfs_schedule.stops, this column is "stop_lon"
                    
    latitude_col: str, column name corresponding to latitude
                    in gtfs_schedule.stops, this column is "stop_lat"
    
    crs: str, coordinate reference system for point geometry
    """
    df = df.assign(
        geometry = gpd.points_from_xy(df[longitude_col], df[latitude_col], 
                                      crs = crs
                                     )
    )

    gdf = gpd.GeoDataFrame(df)
    return gdf