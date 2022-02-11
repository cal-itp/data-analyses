import datetime as dt
import geopandas as gpd
import numpy as np
import pandas as pd

import create_calenviroscreen_lehd_data
import utils
import shared_utils
import warehouse_queries

from calitp.tables import tbl
from siuba import *

DATA_PATH = warehouse_queries.DATA_PATH

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
        departure_time = pd.to_datetime(df.departure_time, errors="coerce"),
        departure_hour = pd.to_datetime(df.departure_time, errors="coerce").dt.hour,
    )
    
    # Any observation with NaTs for departure time get dropped
    # Will create issue later when grouping is done with departure hour
    df = df[df.departure_time.notna()].reset_index(drop=True)
    
    return df


def add_zeros(one_operator_df):
    '''
    Creates rows with 0 trips for hour for all departure hours x shape_ids where that shape_id wasn't running at all
    by first generating a multiindex of all possible shape_id/day_name/departure_hour,
    then setting that as the index for the single-operator shape_frequency df.
    That results in NaNs for departure_hours where no service currently exists,
    then just an easy .fillna(0) turns those rows into useful data for calculating increases.
    '''
    shape_frequency = one_operator_df
    ## insert nulls or 0 for missing values as appropriate
    shapes_routes = (shape_frequency[['shape_id', 'route_id']]
                     .set_index('shape_id').to_dict()['route_id'])
    
    # Currently, day_name_list is ['Thursday', 'Saturday', 'Sunday']...from pre-selected dates
    # Generalize, in case we change it
    day_name_list = list(one_operator_df.day_name.unique())
    iterables = [shape_frequency.shape_id.unique(), day_name_list, range(0, 24)]
    
    multi_ix = pd.MultiIndex.from_product(iterables, 
                                          names=['shape_id', 'day_name', 'departure_hour'])
    shape_frequency = shape_frequency.set_index(['shape_id', 'day_name', 'departure_hour'])
    
    try:
        shape_frequency = shape_frequency.reindex(multi_ix).reset_index()
    except ValueError: ## aggregate rare shape_ids with multiple routes (these may be errors)
        duplicate_indicies = shape_frequency.index[shape_frequency.index.duplicated()]
    #     print(f'''
    #     caution, operator has {len(duplicate_indicies)} duplicate shape/day/departure_hour \
    # out of an ix of {len(shape_frequency.index)}
    #     ''')
        assert len(duplicate_indicies) < len(shape_frequency.index) / 100, 'too many duplicate shape/day/departure_hour'
        shape_frequency = (shape_frequency.groupby(['shape_id', 'day_name', 'departure_hour'])
                           .agg({'calitp_itp_id':max, 
                                 'route_id':max, 
                                 'trips_per_hour':sum, 
                                 'mean_runtime_min':"mean"})
                          )
        
        shape_frequency = shape_frequency.reindex(multi_ix).reset_index()

    shape_frequency['calitp_itp_id'] = (shape_frequency['calitp_itp_id']
                                        .fillna(method='bfill')
                                        .fillna(method='ffill')
                                       )
    shape_frequency['trips_per_hour'] = shape_frequency['trips_per_hour'].fillna(0)
    
    return shape_frequency


def add_all_zeros(shape_frequency_df):
    '''
    Wrapper to apply add_zeros to entire df, fix format
    '''
    df_with_zeros = shape_frequency_df.groupby('calitp_itp_id').apply(add_zeros)
    df_with_zeros = (df_with_zeros.reset_index(drop=True)
             .astype({'calitp_itp_id': 'int64'}) ##fix type
             [['calitp_itp_id', 'shape_id', 'departure_hour',
               'day_name', 'route_id', 'trips_per_hour', 
               'mean_runtime_min']]) ##order to match original
    
    return df_with_zeros


def calculate_runtime_hourlytrips(df):
        
    # Calculate run time for a trip
    # Find the median stop (keep that observation)
    def find_runtime(df):
        mindt = df[df.stop_sequence == df.stop_sequence.min()].departure_time.iloc[0]
        maxdt = df[df.stop_sequence == df.stop_sequence.max()].departure_time.iloc[0]
        td = (maxdt - mindt)
        df['runtime_seconds'] = td.seconds
        return df

    group_cols = ['trip_key', 'day_name']
    df = df.groupby(group_cols).apply(find_runtime)

    df = df.assign(
        middle_stop = df.groupby(group_cols)["stop_sequence"].transform("median"),
    ).astype({"middle_stop": "int64"})
    
    
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
    
    time0 = dt.datetime.now()
    
    dates = warehouse_queries.dates
    
    processed_dfs = {}
    for key in dates.keys():
        start_time_loop = dt.datetime.now()

        print(f"Grab selected trips for {key}, created in warehouse_queries")
        days_st = pd.read_parquet(f"{DATA_PATH}trips_joined_{key}.parquet")

        print(f"Do time calculations for {key}")
        st_trips_joined = get_time_calculations(days_st)
        st_trips_joined.to_parquet(f"{DATA_PATH}timecalc_{key}.parquet")
        
        st_trips_joined = pd.read_parquet(f"{DATA_PATH}timecalc_{key}.parquet")
        print(f"Calculate runtimes for {key}")    
        processed_dfs[key] = calculate_runtime_hourlytrips(st_trips_joined)

        finish_time_loop = dt.datetime.now()
        print(f"Execution time for {key}: {finish_time_loop - start_time_loop}")
    
    
    start_time_append = dt.datetime.now()

    # Append into 1 dataframe and export
    all_operators_shape_frequency = pd.DataFrame()
    for key, value in processed_dfs.items():
        all_operators_shape_frequency = pd.concat([
            all_operators_shape_frequency,
            value], ignore_index=True, axis=0)

    # Add zeros for each hour when existing service doesn't run at all
    all_operators_shape_frequency = add_all_zeros(all_operators_shape_frequency)    
    
    all_operators_shape_frequency.to_parquet(
        f"{DATA_PATH}shape_frequency.parquet")
    
    finish_time_append = dt.datetime.now()
    print(f"Execution time for append/export: {finish_time_append - start_time_append}")
    
    # Attach funding info
    funding_time = dt.datetime.now()
    shape_frequency = pd.read_parquet(f"{DATA_PATH}shape_frequency.parquet")
    
    with_funding = attach_funding(shape_frequency)
    with_funding.to_parquet(f"{DATA_PATH}shape_frequency_funding.parquet")
    
    print(f"Execution time for attaching funding: {funding_time - finish_time_append}")
    print(f"Total execution time: {funding_time - time0}")
    
    # Cleanup local files
    #os.remove(f"{DATA_PATH}trips_*.parquet")
    

## Start functions related to A2
# Categorize tracts and add back further process the operator-route-level df
def generate_shape_categories(shapes_df):
    shapes_df = (shapes_df.reset_index(drop=True)
                 .to_crs(shared_utils.geography_utils.CA_NAD83Albers)
                )
    
    shapes_df = shapes_df.assign(
        geometry = shapes_df.geometry.simplify(tolerance=1),
    )
    
    ## quick fix for invalid geometries?
    ces_df = (create_calenviroscreen_lehd_data.generate_calenviroscreen_lehd_data()
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
    time0 = dt.datetime.now()
    
    # Move creating linestring to this    
    itp_ids = pd.read_parquet(f"{DATA_PATH}shape_frequency.parquet")
    
    itp_ids = list(itp_ids.calitp_itp_id.unique())    
    print(f"Grab ITP IDs")
    print(itp_ids)
    
    all_shapes = shared_utils.geography_utils.make_routes_shapefile(
        ITP_ID_LIST = itp_ids)
    
    time1 = dt.datetime.now()
    print(f"Execution time to make routes shapefile: {time1-time0}")
    
    # Upload to GCS
    shared_utils.utils.geoparquet_gcs_export(all_shapes, DATA_PATH, 'shapes_initial')
    
    all_shapes = gpd.read_parquet(f"{DATA_PATH}shapes_initial.parquet")
    
    time2 = dt.datetime.now()
    processed_shapes = generate_shape_categories(all_shapes)
    processed_shapes = processed_shapes.apply(categorize_shape, axis=1)
        
    print(f"Execution time to categorize routes: {time2-time1}")
    shared_utils.utils.geoparquet_gcs_export(processed_shapes, DATA_PATH, 
                                             'shapes_processed')
    
    print(f"Total execution time: {time2-time0}")
    
   
    
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
    bus_stops = (shared_utils.geography_utils.create_point_geometry(aggregated_stops)
                 .drop(columns = ["stop_lon", "stop_lat"])
                )
    
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
    shared_utils.utils.geoparquet_gcs_export(final, 
                                             utils.GCS_FILE_PATH, 
                                             "bus_stop_times_by_tract")


if __name__ == "__main__":
    # Run this to get the static parquet files
    
    # Get analysis dataset for service increase estimator?
    create_service_estimator_data()
    create_shapes_tract_categorized()
    
    # Get analysis dataset for bus arrivals by tract
    create_bus_arrivals_by_tract_data()