import datetime as dt
import geopandas as gpd
import pandas as pd

import create_calenviroscreen_lehd_data
import utils
import shared_utils

from siuba import *

#------------------------------------------------------------------#
## Functions to create operator-route-level dataset
#------------------------------------------------------------------#
def get_time_calculations(df):
    ## time calculations
    df = df.assign(
        date = pd.to_datetime(df.date),
        departure_time = df.departure_time.dropna().apply(utils.fix_gtfs_time),
    )

    # Something weird comes up trying to generate departure_dt
    # pd.to_datetime() gives today's date
    # datetime.strptime gives year 1900
    # Either way, we have the service date, and later subsetting between 5am-9pm will address this
    df = df.assign(
        departure_time = pd.to_datetime(df.departure_time),
        departure_hour = pd.to_datetime(df.departure_time).dt.hour,
    )
    
    # Any observation with NaTs for departure time get dropped
    # Will create issue later when grouping is done with departure hour
    df = df[df.departure_time.notna()].reset_index(drop=True)
    
    return df


def calculate_runtime_hourlytrips(df):
    # Calculate run time for a trip
    # Find the median stop (keep that observation)
    group_cols = ['trip_key', 'day_name']
    df = df.assign(
        mindt = df.groupby(group_cols)["departure_time"].transform("min"),
        maxdt = df.groupby(group_cols)["departure_time"].transform("max"),
        middle_stop = df.groupby(["trip_key", "day_name"])["stop_sequence"].transform("median"),
    ).astype({"middle_stop": "int64"})

    df = df.assign(
        runtime_seconds = (df.maxdt - df.mindt).dt.seconds
    ).drop(columns = ["mindt", "maxdt"])
    
    # Drop any trips with runtime of NaN calculated
    df = df[df.runtime_seconds.notna()].reset_index(drop=True)

    # Still want to use this to merge on the mean runtime info
    middle_stops = df >> filter(_.stop_sequence == _.middle_stop)
    
    middle_stops = middle_stops.assign(
        mean_runtime_min = (middle_stops.groupby(["calitp_itp_id", 
                                                  "route_id", "shape_id", 
                                                  "departure_hour", "day_name"])
                            ["runtime_seconds"].transform("mean")
                           )
    )
    
    debug_me = middle_stops[middle_stops.mean_runtime_min.isna()][
        ["calitp_itp_id", "shape_id", "trip_key"]]
    print("Debug errors for NaN mean runtimes")
    print(debug_me.head())
    # Why are there some NaNs from this, when NaNs were dropped before?
    # Some are due to no departure_time (handle it above by dropping NaTs)
    
    middle_stops = middle_stops.assign(
        mean_runtime_min = (middle_stops.mean_runtime_min.dropna()     
                            .apply(lambda x: int(round(x) / 60))
                           )
    )   
    
    # Add trips per hour column
    shape_frequency = (
        middle_stops
        >> count(_.calitp_itp_id, _.route_id,
                 _.shape_id, _.departure_hour, _.day_name, sort = True)
        >> rename(trips_per_hour = "n")
        >> inner_join(_, middle_stops, 
                      on = ["calitp_itp_id", "day_name", 
                            "shape_id", "departure_hour", "route_id"])
    )
    
    # Now, data is at the trip-level (trip_key) still present
    # Drop duplicates, but no aggregation because trips_per_hour and mean_runtime 
    # are already correctly generated at the route-level, across trips in that departure hour
    shape_frequency = shape_frequency.drop_duplicates(subset=[
        "calitp_itp_id", "shape_id", "departure_hour",
        "day_name", "route_id"])
    
    # There's an aggregation to deal with multiple route_ids that share same shape_id
    # If there are still multiple route_ids, then aggregate and sum / mean
    # Modify this to include itp_id into the groupby
    shape_frequency2 = (shape_frequency.groupby(
        ["calitp_itp_id", "shape_id", "departure_hour", "day_name"])
                        .agg({"route_id": "max", 
                              "trips_per_hour": "sum", 
                              "mean_runtime_min": "mean"
                             }).reset_index()
                       )
    
    # Now, drop ITP_ID==200 to use individual operator feeds
    shape_frequency3 = shape_frequency2 >> filter(_.calitp_itp_id != 200)
    
    return shape_frequency3


def attach_funding(all_operators_df):
    # This is a small query, can leave it here
    with_funding = (tbl.views.transitstacks()
                    >> select(_.calitp_itp_id == _.itp_id, _.ntd_id, 
                              _.transit_provider, _._5307_funds, _._5311_funds,
                              _.operating_expenses_total_2019)
                    >> collect()
                    >> right_join(_, all_operators_df, on = 'calitp_itp_id')
                   )
    
    def fix_funds(value):
        if type(value) != str:
            return None
        else:
            return int(value.replace('$', '').replace(',', ''))
        
    funding_cols = ["_5307_funds", "_5311_funds", "operating_expenses_total_2019"] 
    for c in funding_cols:
        with_funding[c] = with_funding[c].apply(fix_funds)
    
    return with_funding


# Loop through and grab weekday/Sat/Sun subsets of joined data, calculate runtimes
def create_service_estimator_data():
    #DATA_PATH = "./data/test/"
    DATA_PATH = "gs://calitp-analytics-data/data-analyses/bus_service_increase/test/"
    time0 = dt.datetime.now()
    
    processed_dfs = {}
    for key in dates.keys():
        start_time_loop = dt.datetime.now()

        print(f"Grab selected trips for {key}")
        days_st = pd.read_parquet(f"{DATA_PATH}trips_{key}.parquet")

        print(f"Do time calculations for {key}")
        st_trips_joined = get_time_calculations(days_st)
        #st_trips_joined.to_parquet(f"{DATA_PATH}timecalc_{key}")
        
        print(f"Calculate runtimes for {key}")    
        processed_dfs[key] = calculate_runtime_hourlytrips(st_trips_joined)

        finish_time_loop = dt.datetime.now()
        print(f"Execution time for {key}: {finish_time_loop - start_time_loop}")
    
    
    start_time_append = dt.datetime.now()

    # Append into 1 dataframe and export
    all_operators_shape_frequency = pd.DataFrame()
    for key, value in processed_dfs.items():
        #Difference between concat, append
        #https://stackoverflow.com/questions/15819050/pandas-dataframe-concat-vs-append/48168086
        all_operators_shape_frequency = pd.concat([
            all_operators_shape_frequency,
            value], ignore_index=True, axis=0)

    all_operators_shape_frequency.to_parquet(
        f"{utils.GCS_FILE_PATH}shape_frequency.parquet")
    
    finish_time_append = dt.datetime.now()
    print(f"Execution time for append/export: {finish_time_append - start_time_append}")
    
    # Attach funding info
    funding_time = dt.datetime.now()
    shape_frequency = pd.read_parquet(f"{utils.GCS_FILE_PATH}shape_frequency.parquet")
    
    with_funding = attach_funding(shape_frequency)
    with_funding.to_parquet(f"{utils.GCS_FILE_PATH}shape_frequency_funding.parquet")
    
    print(f"Execution time for attaching funding: {funding_time - finish_time_append}")
    print(f"Total execution time: {funding_time - time0}")
    
    # Cleanup local files
    os.remove(f"{DATA_PATH}trips_*.parquet")
    

#------------------------------------------------------------------#
## Functions to create tract-level dataset
#------------------------------------------------------------------#
def create_bus_arrivals_by_tract_data():
    aggregated_stops_with_geom = pd.read_parquet(
        f"{utils.DATA_PATH}aggregated_stops_with_geom.parquet")

    census_tracts = create_calenviroscreen_lehd_data.generate_calenviroscreen_lehd_data()
    
    # If there are the same stops with multiple lat/lon values
    # Drop duplicates
    aggregated_stops = (aggregated_stops_with_geom
                        .sort_values(["itp_id", "stop_id", "stop_lon", "stop_lat"])
           .drop_duplicates(subset = ["itp_id", "stop_id"])
           .reset_index(drop=True)
    )
    
    print(f"# obs in bus stop arrivals: {len(aggregated_stops_with_geom)}")
    print(f"# obs in bus stop arrivals, no dups lat/lon: {len(aggregated_stops)}")
    
    # Add stop geometry column (default parameter is WGS84)
    bus_stops = shared_utils.geography_utils.create_point_geometry(aggregated_stops).drop(
        columns = ["stop_lon", "stop_lat"])
    
    gdf = gpd.sjoin(
        bus_stops, 
        census_tracts.to_crs(shared_utils.geography_utils.WGS84),
        # Use inner, or else left join will result in some NaN tracts
        how = "inner",
        predicate = "intersects"
    ).drop(columns = "index_right")
    
    # Aggregate by tract level and count bus arrivals, number of stops, etc
    gdf2 = shared_utils.geography_utils.aggregate_by_geography(
        gdf, 
        group_cols = ["Tract"], 
        sum_cols = ["num_arrivals"],
        count_cols = ["stop_id"],
        nunique_cols = ["itp_id"]
    )
    
    # Attach tract geometry back, since our previous spatial join kept bus stop's point geometry
    final = shared_utils.geography_utils.attach_geometry(
        gdf2, census_tracts, 
        merge_col = ["Tract"], join="left"
    )
    
    # Export to GCS
    shared_utils.utils.geoparquet_gcs_export(final, utils.GCS_FILE_PATH, "bus_stop_times_by_tract")


if __name__ == "__main__":
    # Run this to get the static parquet files
    
    # Get analysis dataset for service increase estimator?
    create_service_estimator_data()
    
    # Get analysis dataset for bus arrivals by tract
    create_bus_arrivals_by_tract_data()