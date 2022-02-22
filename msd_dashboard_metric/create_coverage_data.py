import intake
import geopandas as gpd
import os
import pandas as pd
import requests

os.environ["CALITP_BQ_MAX_BYTES"] = str(100_000_000_000)

from calitp.tables import tbl
from calitp import query_sql
from siuba import *

import utils
import shared_utils
import warehouse_queries


catalog = intake.open_catalog("./catalog.yml")


bus_route_types = ['3', '11']

def buffer_by_route_type(row):
    '''
    Buffer bus stops by 800 meters (.5mi),
    rail/ferry by 1600 meters (1mi)
    '''
    if row.route_type in bus_route_types:
        row.geometry = row.geometry.buffer(800)
    else:
        row.geometry = row.geometry.buffer(1600)
    return row


def get_employment_tract_data():
    service_path = 'gs://calitp-analytics-data/data-analyses/bus_service_increase/'
    
    ## Read in processed df from bus_service_increase/B1
    tract_pop_employ = gpd.read_parquet(f'{service_path}bus_stop_times_by_tract.parquet')
    tract_pop_employ = (tract_pop_employ 
                        >> select(-_.num_arrivals, -_.stop_id, -_.itp_id)
                       )
    
    tract_pop_employ = tract_pop_employ.to_crs(
                        shared_utils.geography_utils.CA_NAD83Albers)
    tract_pop_employ['area'] = tract_pop_employ.geometry.area
    
    
    ## option to filter out large tracts (not useful access data...)
    ## 4 sq km threshold
    tract_pop_employ['under_4_sq_km'] = tract_pop_employ.area < 4e+06
    
    ## Print a stat about 
    job_density = (tract_pop_employ >> group_by('under_4_sq_km') 
               >> summarize(jobs = _.num_jobs.sum())
              )
    jobs_proportion = ((job_density >> filter(_.under_4_sq_km) >> select(_.jobs)).sum() / 
                       (job_density >> select(_.jobs)).sum()
                      )
    
    print(f"Proportion of jobs in tracts < 4 sq km: {jobs_proportion}")
    
    ## option to filter out large tracts (not useful access data...)
    ## 4 sq km threshold
    tract_pop_employ['under_4_sq_km'] = tract_pop_employ.area < 4e+06
    ## filter out large tracts
    tract_pop_employ_filtered = tract_pop_employ >> filter(_.under_4_sq_km)
    
    return tract_pop_employ_filtered


def save_initial_data():
    ca_block_joined = utils.get_ca_block_geo()
    shared_utils.utils.geoparquet_gcs_export(ca_block_joined, utils.GCS_FILE_PATH, 
                                             'block_population_joined')
    
    all_stops = utils.get_stops_and_trips(filter_accessible = False)
    all_stops = all_stops.apply(buffer_by_route_type, axis=1)
    shared_utils.utils.geoparquet_gcs_export(all_stops, utils.GCS_FILE_PATH, 
                                             'all_stops')
    
    accessible_stops_trips = utils.get_stops_and_trips(filter_accessible = True)
    accessible_stops_trips = accessible_stops_trips.apply(buffer_by_route_type, axis=1)
    shared_utils.utils.geoparquet_gcs_export(accessible_stops_trips, utils.GCS_FILE_PATH, 
                                             'accessible_stops_trips')
    
    
    # RT Availability
    feed_extract_date = query_sql(
        """
        SELECT
            *,
            PARSE_DATE(
              '%Y-%m-%d',
              REGEXP_EXTRACT(_FILE_NAME, ".*/([0-9]+-[0-9]+-[0-9]+)")
            ) AS extract_date
        FROM gtfs_schedule_history.calitp_feeds_raw
        """
    )
    
    latest = feed_extract_date >> filter(_.extract_date == _.extract_date.max())
    rt_complete = (latest 
               >> filter(-_.gtfs_rt_vehicle_positions_url.isna(),
                         -_.gtfs_rt_service_alerts_url.isna(),
                         -_.gtfs_rt_trip_updates_url.isna())
               >> select(_.calitp_itp_id == _.itp_id, 
                         _.calitp_url_number == _.url_number)
              )

    rt_complete.to_parquet(f'{GCS_FILE_PATH}rt_complete.parquet')
    
    
def save_spatial_joined_data():
    # Read in parquets from above
    ca_block_joined = utils.download_geoparquet(utils.GCS_FILE_PATH, 'block_population_joined')
    all_stops = utils.download_geoparquet(utils.GCS_FILE_PATH, 'all_stops')
    accessible_stops_trips = utils.download_geoparquet(utils.GCS_FILE_PATH, 
                                                       'accessible_stops_trips')
    rt_complete = pd.read_parquet(f"{utils.GCS_FILE_PATH}rt_complete.parquet") 
    
    # Join in GTFS schedule for all stops / accessible stops
    block_level_accessible = (ca_block_joined
                              .sjoin(accessible_stops_trips, how='inner', 
                                 predicate='intersects')
                                ##important at block level to avoid double counts
                              .drop_duplicates(subset=['geo_id'])) 
    
    block_level_static = (ca_block_joined
                          .sjoin(all_stops, how='inner', predicate='intersects')
                        # .drop_duplicates(subset=['geo_id'])
                     ) ## not dropping enables correct feed aggregations...
    
    # Join in with RT data for all stops / accessible stops
    all_stops_rt = (block_level_static 
                >> inner_join(_, rt_complete, 
                              on =['calitp_itp_id', 'calitp_url_number'])
               )
    
    accessible_stops_trips_rt = (block_level_accessible 
                             >> inner_join(_, rt_complete, 
                                           on =['calitp_itp_id', 'calitp_url_number'])
                            )
    # Intermediate exports
    shared_utils.utils.geoparquet_gcs_export(block_level_accessible, utils.GCS_FILE_PATH, 
                                             'block_level_accessible')
    shared_utils.utils.geoparquet_gcs_export(block_level_static, utils.GCS_FILE_PATH, 
                                             'block_level_static')
    shared_utils.utils.geoparquet_gcs_export(all_stops_rt, utils.GCS_FILE_PATH, 
                                             'all_stops_rt')
    shared_utils.utils.geoparquet_gcs_export(accessible_stops_trips_rt, utils.GCS_FILE_PATH, 
                                             'accessible_stops_trips_rt')

    
def employment_spatial_joins(tract_pop_employ_filtered, all_stops, accessible_stops_trips):

    all_employment_joined = (tract_pop_employ_filtered
                    .sjoin(all_stops, how='inner', predicate='intersects')
                    # .drop_duplicates(subset=['Tract'])
                   ) >> select(-_.index_right, -_.index_left) 
    
    accessible_employment_joined = (tract_pop_employ_filtered
                    .sjoin(accessible_stops_trips, how='inner', predicate='intersects')
                    # .drop_duplicates(subset=['Tract'])
                   ) >> select(-_.index_right, -_.index_left)
    
    
    acc_rt_employ = (tract_pop_employ_filtered
                    .sjoin(accessible_stops_trips_rt >> select(-_.index_right, -_.index_left), 
                           how='inner', predicate='intersects')
                    .drop_duplicates(subset=['Tract'])
                   )
    
        
    all_rt_employ = (tract_pop_employ_filtered
                    .sjoin(all_stops_rt >> select(-_.index_right, -_.index_left), 
                           how='inner', predicate='intersects')
                    .drop_duplicates(subset=['Tract'])
                   )
    
    return (all_employment_joined, accessible_employment_joined, 
            acc_rt_employ, all_rt_employ)