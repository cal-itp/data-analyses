"""
Create a schedule stop_times table with direction of travel
between stops.
"""
import dask.dataframe as dd
import dask_geopandas as dg
import datetime
import geopandas as gpd
import numpy as np
import pandas as pd

from calitp_data_analysis import utils
from shared_utils import rt_utils
from segment_speed_utils import helpers
from segment_speed_utils.project_vars import RT_SCHED_GCS, PROJECT_CRS


def prep_scheduled_stop_times(analysis_date: str) -> dg.GeoDataFrame:
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
    )

    trips = helpers.import_scheduled_trips(
        analysis_date,
        columns = ["gtfs_dataset_key", "feed_key", 
                   "trip_id", "trip_instance_key", 
                   "shape_array_key"
                  ],
        get_pandas = True
    )
    
    st_with_trip = dd.merge(
        stop_times,
        trips,
        on = ["feed_key", "trip_id"],
        how = "inner"
    )
    
    st_with_stop = dd.merge(
        st_with_trip,
        stops,
        on = ["feed_key", "stop_id"],
        how = "inner"
    ).drop(columns = ["feed_key", "trip_id"])
    
    st_with_stop = dg.from_dask_dataframe(
        st_with_stop, geometry = "geometry").set_crs(PROJECT_CRS)
    
    return st_with_stop


def find_prior_stop(
    stop_times: dg.GeoDataFrame,
    trip_stop_cols: list
) -> dg.GeoDataFrame:
    """
    For trip-stop, find the previous stop (using stop sequence).
    Attach the previous stop's geometry.
    """
    prior_stop = stop_times[trip_stop_cols].compute()
    
    prior_stop = prior_stop.assign(
        prior_stop_sequence = (
            prior_stop
            .sort_values(["trip_instance_key", "stop_sequence"])
            .groupby("trip_instance_key")
            .stop_sequence
            .shift(1)
        )
    )
    
    prior_stop_geom = stop_times[
        ["trip_instance_key", "stop_sequence", "geometry"]
    ].rename(columns = {
        "stop_sequence": "prior_stop_sequence",
        "geometry": "prior_geometry"
    }).set_geometry("prior_geometry").repartition(npartitions=1)
    
    stop_times_with_prior = dd.merge(
        stop_times,
        prior_stop,
        on = trip_stop_cols,
        how = "left"
    )
    
    stop_times_with_prior_geom = dd.merge(
        stop_times_with_prior,
        prior_stop_geom,
        on = ["trip_instance_key", "prior_stop_sequence"],
        how = "left"
    ).astype({"prior_stop_sequence": "Int64"})
    
    return stop_times_with_prior_geom


def assemble_stop_times_with_direction(analysis_date: str):
    """
    Assemble a stop_times table ready to be joined with 
    RT data (has trip_instance_key).
    For each stop, find the direction it's traveling (prior stop to current stop)
    and attach that as a column.
    The first stop in each trip has direction Unknown.
    """
    start = datetime.datetime.now()

    scheduled_stop_times = prep_scheduled_stop_times(analysis_date).persist()
    
    trip_stop_cols = ["trip_instance_key", "shape_array_key",
                      "stop_id", "stop_sequence"]
    
    scheduled_stop_times2 = find_prior_stop(scheduled_stop_times, trip_stop_cols)
    
    other_stops = scheduled_stop_times2[
        ~(scheduled_stop_times2.prior_geometry.isna())
    ]

    first_stop = scheduled_stop_times2[
        scheduled_stop_times2.prior_geometry.isna()
    ]
    
    first_stop = first_stop.assign(
        stop_primary_direction = "Unknown"
    ).drop(columns = "prior_geometry").compute()
    
    other_stops_no_geom = other_stops.drop(columns = ["prior_geometry"]).compute()
    
    prior_geom = other_stops.prior_geometry.compute()
    current_geom = other_stops.geometry.compute()
        
    stop_direction = np.vectorize(
        rt_utils.primary_cardinal_direction)(prior_geom, current_geom)
    
    other_stops_no_geom = other_stops_no_geom.assign(
        stop_primary_direction = stop_direction 
    )
    
    scheduled_stop_times_with_direction = pd.concat(
        [first_stop, other_stops_no_geom], 
        axis=0
    )
    
    df = scheduled_stop_times_with_direction.sort_values([
        "trip_instance_key", "stop_sequence"]
    ).reset_index(drop=True)

    time1 = datetime.datetime.now()
    print(f"get scheduled stop times with direction: {time1 - start}")
    
    utils.geoparquet_gcs_export(
        df,
        RT_SCHED_GCS,
        f"stop_times_direction_{analysis_date}"
    )
    
    
    
    end = datetime.datetime.now()
    print(f"execution time: {end - start}")
    
    return


if __name__ == "__main__":  
    
    from update_vars import analysis_date_list
    
    for date in analysis_date_list:
        assemble_stop_times_with_direction(date)