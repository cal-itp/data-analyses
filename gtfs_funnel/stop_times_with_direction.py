"""
Create a schedule stop_times table with direction of travel
between stops.
"""
import datetime
import geopandas as gpd
import numpy as np
import pandas as pd

from dask import delayed, compute
from typing import Literal

from calitp_data_analysis import utils
from shared_utils import rt_utils
from segment_speed_utils import helpers, wrangle_shapes
from segment_speed_utils.project_vars import RT_SCHED_GCS, PROJECT_CRS


def prep_scheduled_stop_times(analysis_date: str) -> gpd.GeoDataFrame:
    """
    Attach stop geometry to stop_times data, and also 
    add in trip_instance_key (from trips table), so 
    this can be joined to any RT dataset.
    """
    stops = helpers.import_scheduled_stops(
        analysis_date,
        columns = ["feed_key", "stop_id", "geometry"],
        crs = PROJECT_CRS,
        get_pandas = True
    )

    stop_times = helpers.import_scheduled_stop_times(
        analysis_date,
        columns = ["feed_key", "trip_id", "stop_id", "stop_sequence"]
    ).compute()

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
    ).drop(columns = ["feed_key", "trip_id"])
    
    st_with_stop = gpd.GeoDataFrame(
        st_with_stop, geometry = "geometry", crs = PROJECT_CRS)
    
    return st_with_stop


def get_projected_stop_meters(
    stop_times: pd.DataFrame, 
    shapes: gpd.GeoDataFrame
) -> pd.DataFrame:
    """
    Project the stop's position to the shape and
    get stop_meters (meters from start of the shape).
    """
    gdf = pd.merge(
        stop_times,
        shapes.rename(columns = {"geometry": "shape_geometry"}),
        on = "shape_array_key",
        how = "inner"
    )
    
    gdf = gdf.assign(
        stop_meters = gdf.shape_geometry.project(gdf.geometry)
    ).drop(columns = "shape_geometry").drop_duplicates()
    
    return gdf
    

def find_prior_stop(
    stop_times: dg.GeoDataFrame,
    trip_stop_cols: list
) -> dg.GeoDataFrame:
    """
    For trip-stop, find the previous stop (using stop sequence).
    Attach the previous stop's geometry.
    """
    prior_stop = stop_times[trip_stop_cols].sort_values(
        trip_stop_cols).reset_index(drop=True)
        
    prior_stop = prior_stop.assign(
        prior_stop_sequence = (prior_stop.groupby("trip_instance_key")
                               .stop_sequence.shift(1)),
    )
                     
    
    prior_stop_geom = (stop_times[trip_stop_cols + ["geometry"]]
                       .rename(columns = {
                           "stop_sequence": "prior_stop_sequence",
                           "stop_id": "prior_stop_id",
                           "geometry": "prior_geometry"
                           })
                       .set_geometry("prior_geometry")
                      )
    
    stop_times_with_prior = pd.merge(
        stop_times,
        prior_stop,
        on = trip_stop_cols,
        how = "left"
    )
    
    stop_times_with_prior_geom = pd.merge(
        stop_times_with_prior,
        prior_stop_geom,
        on = ["trip_instance_key", "prior_stop_sequence"],
        how = "left"
    ).astype({
        "prior_stop_sequence": "Int64",
    }).fillna({"prior_stop_id": ""})
    
    
    stop_times_with_prior_geom = stop_times_with_prior_geom.assign(
        stop_pair = stop_times_with_prior_geom.apply(
            lambda x: 
            str(x.prior_stop_id) + "-" + str(x.stop_id), 
            axis=1, 
        )
    ).drop(columns = "prior_stop_id")
    
    return stop_times_with_prior_geom


def assemble_stop_times_with_direction(analysis_date: str, dict_inputs: dict):
    """
    Assemble a stop_times table ready to be joined with 
    RT data (has trip_instance_key).
    For each stop, find the direction it's traveling (prior stop to current stop)
    and attach that as a column.
    The first stop in each trip has direction Unknown.
    """
    start = datetime.datetime.now()

    EXPORT_FILE = dict_inputs["stop_times_direction_file"]
    
    scheduled_stop_times = prep_scheduled_stop_times(analysis_date)

    trip_stop_cols = ["trip_instance_key", "stop_sequence", "stop_id"]
        
    scheduled_stop_times2 = find_prior_stop(
        scheduled_stop_times, trip_stop_cols
    )
    
    other_stops = scheduled_stop_times2[
        ~(scheduled_stop_times2.prior_geometry.isna())
    ]

    first_stop = scheduled_stop_times2[
        scheduled_stop_times2.prior_geometry.isna()
    ]
    
    first_stop = first_stop.assign(
        stop_primary_direction = "Unknown"
    ).drop(columns = "prior_geometry")
    
    other_stops_no_geom = other_stops.drop(columns = ["prior_geometry"])
    
    prior_geom = other_stops.prior_geometry
    current_geom = other_stops.geometry
    
    # Create a column with readable direction like westbound, eastbound, etc
    stop_direction = np.vectorize(
        rt_utils.primary_cardinal_direction)(prior_geom, current_geom)
    stop_distance = prior_geom.distance(current_geom)
    
    # Create a column with normalized direction vector
    # Add this because some bus can travel in southeasterly direction, 
    # but it's categorized as southbound or eastbound depending 
    # on whether south or east value is larger.
    # Keeping the normalized x/y direction allows us to distinguish a bit better later
    direction_vector = wrangle_shapes.get_direction_vector(prior_geom, current_geom)
    normalized_vector = wrangle_shapes.get_normalized_vector(direction_vector)
    
    other_stops_no_geom = other_stops_no_geom.assign(
        stop_primary_direction = stop_direction,
        stop_meters = stop_distance,
        # since we can't save tuples, let's assign x, y normalized direction vector
        # as 2 columns
        stop_dir_xnorm = normalized_vector[0],
        stop_dir_ynorm = normalized_vector[1]
    )
    
    scheduled_stop_times_with_direction = pd.concat(
        [first_stop, other_stops_no_geom], 
        axis=0
    )
        
    df = scheduled_stop_times_with_direction.sort_values(
        trip_stop_cols).reset_index(drop=True)

    time1 = datetime.datetime.now()
    print(f"get scheduled stop times with direction: {time1 - start}")
    
    utils.geoparquet_gcs_export(
        df,
        RT_SCHED_GCS,
        f"{EXPORT_FILE}_{analysis_date}"
    )
    
    end = datetime.datetime.now()
    print(f"execution time: {end - start}")
    
    del scheduled_stop_times, scheduled_stop_times2
    del other_stops_no_geom, scheduled_stop_times_with_direction, df
    
    return


if __name__ == "__main__":  
    
    from update_vars import analysis_date_list, CONFIG_DICT

    for date in analysis_date_list:
        assemble_stop_times_with_direction(date, CONFIG_DICT)