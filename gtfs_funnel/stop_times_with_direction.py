"""
Create a schedule stop_times table with direction of travel
between stops.
"""
import dask.dataframe as dd
import datetime
import geopandas as gpd
import numpy as np
import pandas as pd

from calitp_data_analysis import utils
from shared_utils import rt_utils
from segment_speed_utils import helpers, wrangle_shapes
from segment_speed_utils.project_vars import PROJECT_CRS
from update_vars import GTFS_DATA_DICT
RT_SCHED_GCS = GTFS_DATA_DICT.gcs_paths.RT_SCHED_GCS

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
    

def find_prior_subseq_stop(
    stop_times: gpd.GeoDataFrame,
    trip_stop_cols: list
) -> gpd.GeoDataFrame:
    """
    For trip-stop, find the previous stop (using stop sequence).
    Attach the previous stop's geometry.
    This will determine the direction for the stop (it's from prior stop).
    Add in subseq stop information too.
    """
    prior_stop = stop_times[trip_stop_cols].sort_values(
        trip_stop_cols).reset_index(drop=True)
        
    prior_stop = prior_stop.assign(
        prior_stop_sequence = (prior_stop.groupby("trip_instance_key")
                               .stop_sequence
                               .shift(1)),
        # add subseq stop info here
        subseq_stop_sequence = (prior_stop.groupby("trip_instance_key")
                                .stop_sequence
                                .shift(-1)),
        subseq_stop_id = (prior_stop.groupby("trip_instance_key")
                          .stop_id
                          .shift(-1)),
        subseq_stop_name = (prior_stop.groupby("trip_instance_key")
                          .stop_name
                          .shift(-1))        
    )
    
    # Merge in prior stop geom as a separate column so we can
    # calculate distance / direction
    prior_stop_geom = (stop_times[["trip_instance_key", 
                                   "stop_sequence", "geometry"]]
                       .rename(columns = {
                           "stop_sequence": "prior_stop_sequence",
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
        "subseq_stop_sequence": "Int64"
    }).fillna({
        "subseq_stop_id": "",
        "subseq_stop_name": ""
    })
        
    # Create stop pair with underscores, since stop_id 
    # can contain hyphens
    stop_times_with_prior_geom = stop_times_with_prior_geom.assign(
        stop_pair = stop_times_with_prior_geom.apply(
            lambda x: 
            str(x.stop_id) + "__" + str(x.subseq_stop_id), 
            axis=1, 
        ),
        stop_pair_name = stop_times_with_prior_geom.apply(
            lambda x: 
            x.stop_name + "__" + x.subseq_stop_name, 
            axis=1, 
        ),    
    ).drop(columns = ["subseq_stop_id", "subseq_stop_name"])
    
    return stop_times_with_prior_geom


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

    trip_stop_cols = ["trip_instance_key", "stop_sequence", 
                      "stop_id", "stop_name"]
        
    scheduled_stop_times2 = find_prior_subseq_stop(
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
    
    other_stops_no_geom = other_stops_no_geom.assign(
        stop_primary_direction = stop_direction,
        stop_meters = stop_distance,
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
        
    return


if __name__ == "__main__":  
    
    from update_vars import analysis_date_list

    for date in analysis_date_list:
        print(date)
        assemble_stop_times_with_direction(date, GTFS_DATA_DICT)