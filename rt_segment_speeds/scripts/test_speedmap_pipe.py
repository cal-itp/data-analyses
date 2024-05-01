import datetime
import geopandas as gpd
import pandas as pd
import shapely
import sys

from loguru import logger
from pathlib import Path
from typing import Optional, Literal, Union

from update_vars import SEGMENT_GCS, GTFS_DATA_DICT
from calitp_data_analysis import utils, geography_utils
from segment_speed_utils import helpers, neighbor
import interpolate_stop_arrival
import stop_arrivals_to_speed


def cut_longer_segments(
    stop_segments: gpd.GeoDataFrame, 
    segment_length: int
) -> gpd.GeoDataFrame:
    """
    """
    gdf = stop_segments.loc[
        stop_segments.segment_length > segment_length
    ]
                
    gdf["segment_geometry"] = gdf.apply(
        lambda x:
        geography_utils.create_segments(x.geometry, int(segment_length)),
        axis=1
    )
    
    gdf2 = geography_utils.explode_segments(
        gdf,
        group_cols = ['trip_instance_key'],
        segment_col = 'segment_geometry'
    )
    
    trip_stop_cols = ["trip_instance_key", "stop_sequence"]

    gdf2 = gdf2.assign(
        segment_sequence2 = gdf2.groupby(trip_stop_cols).cumcount()
    )
    
    # Amend segment_id which has suffix "-1"
    # after we explode, the suffix needs to increase, -1, -2, -3
    gdf2 = gdf2.assign(
        # split off the last hyphen and add new suffix (segment_sequence)
        segment_id = (gdf2.segment_id
               .str.rsplit('-1', n=1, expand=True)[0] +
               "-" + gdf2.segment_sequence2.astype(str)                
              )
    )
    
    # TODO: this might be unnecessarily complicated
    # leave for now, but maybe we can get away with just segment_sequence2
    # although for aggregation, we want to ensure segments have same endpoints
    # if we want to stack it, and maybe segment_sequence2 isn't sufficient?
    # we don't want to stack segment1 with segment1 
    
    # To get a new stop_sequence that is numeric, 
    # would have to calculate cumulative distance in the segment now
    gdf2["seg_length"] = gdf2.geometry.length
    gdf2["prev_seg_length"] = (gdf2.groupby(trip_stop_cols)
                               .seg_length
                               .shift(1)
                              )
    
    gdf2["seg_cumulative"] = (gdf2.groupby(trip_stop_cols)
                              .prev_seg_length
                              .cumsum()
                             )
    
    gdf2["seg_pct"] = gdf2.seg_cumulative.divide(
        gdf2.segment_length).round(2)
    
    keep_cols = stop_segments.columns.tolist()
    
    gdf3 = gdf2.assign(
        stop_sequence1 = (gdf2.stop_sequence + gdf2.seg_pct).fillna(
            gdf2.stop_sequence)
    )[keep_cols + ["stop_sequence1"]] 

    return gdf3


def get_proxy_stops(
    longer_segments: gpd.GeoDataFrame
) -> gpd.GeoDataFrame: 
    # todo: update references to shapely.Point(x.geometry.coords[0])
    # we can use shapely.get_point()
    keep_cols =  ["trip_instance_key", "shape_array_key",
                   "stop_sequence", "stop_id", "stop_pair", 
                   #"stop_primary_direction",
                   "geometry"]

    proxy_stops = longer_segments.assign(
        geometry = longer_segments.apply(
            lambda x: shapely.get_point(x.geometry, 0), axis=1)
    ).rename(
        columns = {"stop_id1": "stop_id"}
    )[keep_cols + ["stop_sequence1"]].to_crs("EPSG:4326")

    # stop_primary_direction can be populated when it's appended
    # with the stop_times, and we can sort by trip-stop_sequence1
    # and pd.ffill (forward fill)
    
    return proxy_stops


def concatenate_new_stops_with_existing(
    new_stops: gpd.GeoDataFrame,
    analysis_date: str
) -> gpd.GeoDataFrame: 
    stop_times = helpers.import_scheduled_stop_times(
        analysis_date,
        columns = ["trip_instance_key", "shape_array_key",
                   "stop_sequence", "stop_id", "stop_pair", 
                   "stop_primary_direction",
                   "geometry"],
        with_direction = True,
        get_pandas = True,
        crs = "EPSG:4326",
    )
    
    # need to check whether CRS is 3310 when it goes into nearest neighbor
    # right now, set it at 4326
    
    trip_stop_cols = ["trip_instance_key", "stop_sequence"]
    gdf = pd.concat(
        [stop_times, new_stops], 
        axis=0, ignore_index=True
    ).sort_values(
        trip_stop_cols
    ).reset_index(drop=True)
    
    gdf = gdf.assign(
        stop_primary_direction = (gdf.groupby(trip_stop_cols)
                                  .stop_primary_direction
                                  .ffill()
                                 ),
        stop_sequence1 = gdf.stop_sequence1.fillna(gdf.stop_sequence),
        #TODO: create stop_sequence2 
        # is this needed or will stop_sequence1 be sufficient
        # segments go from current stop to next stop, 
    )
    
    return gdf

def nearest_neighbor_for_stop(
    analysis_date: str,
    segment_type: str,
    config_path: Optional[Path] = GTFS_DATA_DICT
):
    """
    Set up nearest neighbors for RT stop times, which
    includes all trips. Use stop sequences for each trip.
    """
    
    dict_inputs = config_path[segment_type]
    
    start = datetime.datetime.now()
    EXPORT_FILE = f'{dict_inputs["stage2"]}'
    
    stop_time_col_order = [
        'trip_instance_key', 'shape_array_key',
        'stop_sequence', 'stop_sequence1', 
        'stop_id', 'stop_pair',
        'stop_primary_direction', 'geometry'
    ] 
        
    
    stop_times = gpd.read_parquet(
        f"{SEGMENT_GCS}stop_time_expansion/"
        f"speedmap_stop_times_{analysis_date}.parquet",
    )

    stop_times = stop_times.reindex(columns = stop_time_col_order)
    
    gdf = neighbor.merge_stop_vp_for_nearest_neighbor(
        stop_times, analysis_date)
        
    results = neighbor.add_nearest_neighbor_result(gdf, analysis_date)
    
    utils.geoparquet_gcs_export(
        results,
        SEGMENT_GCS,
        f"{EXPORT_FILE}_{analysis_date}",
    )
    
    
    end = datetime.datetime.now()
    logger.info(f"nearest neighbor for {segment_type} "
                f"{analysis_date}: {end - start}")
    
    return



if __name__ == "__main__":
        
    LOG_FILE = "../logs/nearest_vp.log"
    logger.add(LOG_FILE, retention="3 months")
    logger.add(sys.stderr, 
               format="{time:YYYY-MM-DD at HH:mm:ss} | {level} | {message}", 
               level="INFO") 
    
    #from segment_speed_utils.project_vars import analysis_date_list
    from shared_utils import rt_dates
    
    for analysis_date in [rt_dates.DATES["mar2024"]]:
        
        start = datetime.datetime.now()
        SEGMENT_LENGTH = 1_000
        
        stop_segments = gpd.read_parquet(
            f"{SEGMENT_GCS}segment_options/"
            f"stop_segments_{analysis_date}.parquet",
        )

        stop_segments = stop_segments.assign(
            segment_length = stop_segments.geometry.length
        )
        
        print(stop_segments.shape)
        
        longer_segments = cut_longer_segments(
            stop_segments, SEGMENT_LENGTH)

        time1 = datetime.datetime.now()
        print(f"cut longer segments: {time1 - start}")
        
        proxy_stops = get_proxy_stops(longer_segments)
        
        time2 = datetime.datetime.now()
        print(f"get proxy stops: {time2 - time1}")
        
        new_stop_times = concatenate_new_stops_with_existing(
            proxy_stops,
            analysis_date
        )
        
        utils.geoparquet_gcs_export(
            new_stop_times,
            SEGMENT_GCS,
            f"stop_time_expansion/speedmap_stop_times_{analysis_date}"
        )
        
        del new_stop_times, proxy_stops
        
        time3 = datetime.datetime.now()
        print(f"concatenate and export new stop times: {time3 - time2}")
        
        speedmap_segments = pd.concat([
            stop_segments.loc[stop_segments.segment_length < SEGMENT_LENGTH],
            longer_segments], 
            axis=0
        ).sort_values(
            ["schedule_gtfs_dataset_key", 
            "trip_instance_key", "stop_sequence"]
        ).reset_index(drop=True).drop(
            columns = ["segment_length"])
        
        # need to fill in missing because when we concat,
        # the segments that were shorter do not have values for stop_sequence1
        speedmap_segments = speedmap_segments.assign(
            stop_sequence1 = speedmap_segments.stop_sequence1.fillna(
                speedmap_segments.stop_sequence)
        )
        
        utils.geoparquet_gcs_export(
            speedmap_segments,
            SEGMENT_GCS,
            f"segment_options/speedmap_segments_{analysis_date}"
        )
        
        del speedmap_segments, stop_segments, longer_segments
        
        time4 = datetime.datetime.now()
        print(f"concatenate segments and export: {time4 - time3}")
        
        segment_type = "speedmap_segments"
        
        nearest_neighbor_for_stop(
            analysis_date = analysis_date,
            segment_type = segment_type,
            config_path = GTFS_DATA_DICT
        ) 
        
        interpolate_stop_arrival.interpolate_stop_arrivals(
            analysis_date = analysis_date, 
            segment_type = segment_type, 
            config_path = GTFS_DATA_DICT
        )
        
        stop_arrivals_to_speed.calculate_speed_from_stop_arrivals(
            analysis_date = analysis_date, 
            segment_type = segment_type, 
            config_path = GTFS_DATA_DICT
        )