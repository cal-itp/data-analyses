"""
Create a schedule stop_times table with direction of travel
between stops.
"""
import datetime
import geopandas as gpd
import numpy as np
import pandas as pd
import sys

from loguru import logger

from calitp_data_analysis import utils
from shared_utils import rt_utils
from segment_speed_utils import helpers
from segment_speed_utils.project_vars import PROJECT_CRS
from update_vars import GTFS_DATA_DICT, RT_SCHED_GCS

def prep_scheduled_stop_times(analysis_date: str) -> gpd.GeoDataFrame:
    """
    Attach stop geometry to stop_times data, and also 
    add in trip_instance_key (from trips table), so 
    this can be joined to any RT dataset.
    """
    stops = helpers.import_scheduled_stops(
        analysis_date,
        columns = ["feed_key", "stop_id", "stop_name", "geometry"],
        crs = PROJECT_CRS,
        get_pandas = True
    )

    stop_times = helpers.import_scheduled_stop_times(
        analysis_date,
        columns = ["feed_key", "trip_id", "stop_id", "stop_sequence"],
        get_pandas = True
    )

    trips = helpers.import_scheduled_trips(
        analysis_date,
        columns = ["gtfs_dataset_key", "feed_key", 
                   "trip_id", "trip_instance_key", 
                   "shape_array_key"
                  ],
        get_pandas = True
    )
    
    st_with_trip = pd.merge(
        stop_times,
        trips,
        on = ["feed_key", "trip_id"],
        how = "inner"
    )
    
    st_with_stop = pd.merge(
        st_with_trip,
        stops,
        on = ["feed_key", "stop_id"],
        how = "inner"
    ).drop(columns = ["trip_id"])
    
    st_with_stop = gpd.GeoDataFrame(
        st_with_stop, geometry = "geometry", crs = PROJECT_CRS
    )
    
    return st_with_stop


def get_projected_stop_meters(
    stop_times: gpd.GeoDataFrame,
    analysis_date: str,
) -> pd.Series:
    """
    Project the stop's position to the shape and
    get stop_meters (meters from start of the shape).
    Only return stop_meters as pd.Series to use as a column later. 
    """
    shapes = helpers.import_scheduled_shapes(
        analysis_date, 
        columns = ["shape_array_key", "geometry"],
        crs = PROJECT_CRS,
        get_pandas=True
    ).dropna(subset="geometry")
    
    gdf = pd.merge(
        stop_times.to_crs(PROJECT_CRS),
        shapes.to_crs(PROJECT_CRS).rename(columns = {"geometry": "shape_geometry"}),
        on = "shape_array_key",
        how = "inner"
    ).set_geometry("geometry")
    
    stop_meters = gdf.shape_geometry.project(gdf.geometry)

    return stop_meters
    

def find_prior_subseq_stop_info(
    stop_times: gpd.GeoDataFrame, 
    analysis_date: str,
    trip_cols: list = ["trip_instance_key"],
    trip_stop_cols: list = ["trip_instance_key", "stop_sequence"]
) -> gpd.GeoDataFrame:
    """
    For trip-stop, find the previous stop (using stop sequence).
    Attach the previous stop's geometry.
    This will determine the direction for the stop (it's from prior stop).
    Add in subseq stop information too.
    
    Create columns related to comparing current to prior stop.
    - stop_pair (stop_id1_stop_id2)
    - stop_pair_name (stop_name1__stop_name2)
    """
    stop_meters = get_projected_stop_meters(stop_times, analysis_date)
    
    gdf = stop_times[
        trip_stop_cols + ["stop_id", "stop_name", "geometry"]
    ].assign(
        stop_meters = stop_meters
    )
    gdf = gdf.sort_values(trip_stop_cols) #  important! gdf loses sort after prep_scheduled_stop_times
    #  slow check , commenting out since we're sorting right here (is there a faster way to test?)
    # check_monotonic = gdf.groupby(trip_cols).stop_sequence.apply(lambda x: x.is_monotonic_increasing)
    # assert check_monotonic.all(), 'gdf must be sorted by trip_instance_key, stop_sequence'
    gdf = gdf.assign(
        prior_geometry = (gdf.groupby(trip_cols)
                          .geometry
                          .shift(1)),
        prior_stop_sequence = (gdf.groupby(trip_cols)
                               .stop_sequence
                               .shift(1)),
        # add subseq stop info here
        subseq_stop_sequence = (gdf.groupby(trip_cols)
                                .stop_sequence
                                .shift(-1)),
        subseq_stop_id = (gdf.groupby(trip_cols)
                          .stop_id
                          .shift(-1)),
        subseq_stop_name = (gdf.groupby(trip_cols)
                          .stop_name
                          .shift(-1)),
    ).fillna({
        **{c: "" for c in ["subseq_stop_id", "subseq_stop_name"]}
    })
    

    stop_direction = np.vectorize(rt_utils.primary_cardinal_direction)(
        gdf.prior_geometry.fillna(gdf.geometry), gdf.geometry)
    
    # Just keep subset of columns because we'll get other stop columns back when we merge with stop_times
    keep_cols = [
        "trip_instance_key", "stop_sequence",
        "stop_meters",
        "prior_stop_sequence", "subseq_stop_sequence"
    ]
    
    # Create stop pair with underscores, since stop_id 
    # can contain hyphens
    gdf2 = gdf[keep_cols].assign(
        stop_primary_direction = stop_direction,
        stop_pair = gdf.stop_id.astype(str).str.cat(
            gdf.subseq_stop_id.astype(str), sep = "__"),
        stop_pair_name = gdf.stop_name.astype(str).str.cat(
            gdf.subseq_stop_name.astype(str), sep = "__"),
    )
    
    stop_times_geom_direction = pd.merge(
        stop_times,
        gdf2,
        on = trip_stop_cols,
        how = "inner"
    )

    return stop_times_geom_direction 


def assemble_stop_times_with_direction(
    analysis_date: str, 
    dict_inputs: dict
):
    """
    Assemble a stop_times table ready to be joined with 
    RT data (has trip_instance_key).
    For each stop, find the direction it's traveling 
    (prior stop to current stop)
    and attach that as a column.
    The first stop in each trip has direction Unknown.
    """
    start = datetime.datetime.now()

    EXPORT_FILE = dict_inputs.rt_vs_schedule_tables.stop_times_direction
    
    scheduled_stop_times = prep_scheduled_stop_times(analysis_date)

    trip_cols = ["trip_instance_key"]
    trip_stop_cols = ["trip_instance_key", "stop_sequence"]
        
    df = find_prior_subseq_stop_info(
        scheduled_stop_times,
        analysis_date,
        trip_cols = trip_cols,
        trip_stop_cols = trip_stop_cols
    ).sort_values(
        trip_stop_cols
    ).reset_index(drop=True)
    
    utils.geoparquet_gcs_export(
        df,
        RT_SCHED_GCS,
        f"{EXPORT_FILE}_{analysis_date}"
    )
    
    end = datetime.datetime.now()
    logger.info(
        f"scheduled stop times with direction {analysis_date}: {end - start}"
    )
        
    return


if __name__ == "__main__":  
    
    from update_vars import analysis_date_list
    
    LOG_FILE = "./logs/preprocessing.log"
    logger.add(LOG_FILE, retention="3 months")
    logger.add(sys.stderr, 
               format="{time:YYYY-MM-DD at HH:mm:ss} | {level} | {message}", 
               level="INFO")
        
    for date in analysis_date_list:
        assemble_stop_times_with_direction(date, GTFS_DATA_DICT)