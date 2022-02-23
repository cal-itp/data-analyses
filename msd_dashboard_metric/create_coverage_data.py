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
    

def make_tract_block_crosswalk(block_df, tract_df):
    # Use overlay
    # See how much of block intersects with tract
    # Keep the largest intersection
    crosswalk = gpd.overlay(
        block_df[["geo_id", "geometry"]].assign(block_area = block_df.geometry.area),
        tract_df[["Tract", "geometry"]],
        how = 'intersection',
    )

    crosswalk2 = crosswalk.assign(
        overlap_area = crosswalk.geometry.area
    )
    
    crosswalk2 = (crosswalk2.sort_values(['geo_id', 'overlap_area'], 
                                     ascending=[True, False])
              .drop_duplicates(subset=['geo_id'])
              .drop(columns = ['block_area', 'overlap_area', 'geometry'])
              .reset_index(drop=True)
             )
    
    return crosswalk2


def spatial_join_to_stops(ca_block, stops_dfs, rt_df):
    """
    ca_block: pandas.DataFrame
        base geography file, by blocks
    stop_df: pandas.DataFrame
        all stops, stops that are accessible, etc 
    """
    
    # Store all the sjoins in this dict
    processed_dfs = {}
    
    for stop_key, stop_df in stops_dfs.items():
        # Join in GTFS schedule for all stops / accessible stops for blocks
        df = (ca_block.sjoin(stop_df, how = 'inner', predicate='intersects')
              .drop(columns = 'index_right')
              #.rename(columns = {"index_right": f"index_{stop_key}"})
             )

        if stop_key=="accessible_stops":
            ##important at block level to avoid double counts
            df = df.drop_duplicates(subset=['geo_id'])

        key = f"block_{stop_key}"
        processed_dfs[key] = df

        # Join in RT availability
        df2 = (df 
               >> inner_join(_, rt_df, 
                             on = ['calitp_itp_id', 'calitp_url_number'])
              )
            
        rt_key = f"block_{stop_key}_rt"
        processed_dfs[rt_key] = df2
                       
    return processed_dfs


def employment_spatial_joins(tract_employ_df, stop_dfs, crosswalk_block_tract):
    """
    tract_employ_df: pandas.DataFrame
        base geography file, by tracts
    stop_df: pandas.DataFrame
        all stops, stops that are accessible, etc 
    """
    
    # Store all the sjoins in this dict
    processed_dfs = {}    
    
    for stop_key, stop_df in stop_dfs.items():
        if "rt" not in stop_key:
            df = (tract_employ_df.sjoin(stop_df, how='inner', predicate='intersects')
                  .drop(columns = 'index_right')
                 )
            
        if "rt" in stop_key:
            # With RT data, the block geometry is included
            # Use crosswalk to merge
            
            # First, merge in crosswalk to get the block's geo_id
            df = pd.merge(
                tract_employ_df,
                crosswalk_block_tract,
                on = "Tract",
                how = "inner"
            )
            
            # Now, merge in block level data with geo_id
            df = pd.merge(df,
                          stop_df.drop(columns = ["area", "geometry"]),
                          on = 'geo_id',
                          how = 'inner'
            )
            # Drop duplicates because there are multiple blocks in a tract
            # But, we only keep the 1 stop linked to the 1 tract
            df = df.drop_duplicates(subset=['Tract'])
        
        key = f"tract_{stop_key.replace('block_', '')}"
        processed_dfs[key] = df
    
    return processed_dfs


def spatial_joins_to_blocks_and_tracts():    
    # Read in parquets from above
    ca_block_joined = shared_utils.utils.download_geoparquet(
        utils.GCS_FILE_PATH, 'block_population_joined')
    all_stops = shared_utils.utils.download_geoparquet(utils.GCS_FILE_PATH, 'all_stops')
    accessible_stops_trips = shared_utils.utils.download_geoparquet(
        utils.GCS_FILE_PATH, 'accessible_stops_trips')
    rt_complete = pd.read_parquet(f"{utils.GCS_FILE_PATH}rt_complete.parquet") 
    
    # Read in employment data by tract
    tract_pop_employ_filtered = get_employment_tract_data()
    
    # Do 1st spatial join 
    # Blocks to all stops / accessible stops
    stops_dfs = {
        "all_stops": all_stops,
        "accessible_stops": accessible_stops_trips,
    }

    sjoin_blocks = spatial_join_to_stops(ca_block_joined, stops_dfs, rt_complete)
    
    # Save intermediate exports?
    # Skip for now, since dict is holding results
    
    # Make tract-block crosswalk
    crosswalk = make_tract_block_crosswalk(ca_block_joined, tract_pop_employ_filtered)
    
    stops_dfs2 = {
        "all_stops": all_stops,
        "accessible_stops": accessible_stops_trips,
        "all_stops_rt": sjoin_blocks["block_all_stops_rt"],
        "accessible_stops_rt": sjoin_blocks["block_accessible_stops_rt"],
    }

    sjoin_tracts = employment_spatial_joins(tract_pop_employ_filtered, stops_dfs2, crosswalk)
    
    return sjoin_blocks, sjoin_tracts