"""
"""
import dask.dataframe as dd
import dask_geopandas as dg
import datetime as dt
import geopandas as gpd
import intake
import pandas as pd

from bus_service_utils import utils as bus_utils
from calitp_data_analysis import geography_utils, utils
from segment_speed_utils import helpers
from shared_utils import portfolio_utils
from warehouse_queries import DATA_PATH

catalog = intake.open_catalog("*.yml")

#------------------------------------------------------------------#
## Functions to create operator-route-level dataset
#------------------------------------------------------------------#

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

        assert len(duplicate_indicies) < len(shape_frequency.index) / 100, 'too many duplicate shape/day/departure_hour'
        shape_frequency = (shape_frequency
                           .groupby(['shape_id', 'day_name', 'departure_hour'])
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


def frequency_by_shape(df: pd.DataFrame) -> pd.DataFrame:
        
    trips_per_hour = (df
                      .groupby(["schedule_gtfs_dataset_key", 
                                "day_name", "departure_hour", 
                                "time_of_day",
                                "route_id", "shape_id"], 
                              observed=True, group_keys=False)
                      .agg({
                          "trip_instance_key": "count",
                          "service_minutes": "mean"
                      })
                      .reset_index()
                      .rename(columns = {
                          "trip_instance_key": "n_trips",
                          "service_minutes": "avg_service_minutes"
                      })
                     )

    # Should we deal with multiple route_ids that share the same shape_id?
    # Used to...but does that still happen in v2?
     
    return trips_per_hour


def attach_funding(all_operators_df):
    # This is a small query, can leave it here
    with_funding = (tbl.views.transitstacks()
                    >> select(_.calitp_itp_id == _.itp_id, _.ntd_id, 
                              _.transit_provider, _._5307_funds, _._5311_funds,
                              _.operating_expenses_total_2019)
                    >> collect()
                    >> right_join(_, all_operators_df, on = 'calitp_itp_id')
                   )
    
    def fix_funds(value: str) -> int:
        if type(value) != str:
            return None
        else:
            return int(value.replace('$', '').replace(',', ''))
        
    funding_cols = ["_5307_funds", "_5311_funds", "operating_expenses_total_2019"] 
    for c in funding_cols:
        with_funding[c] = with_funding[c].apply(fix_funds)
    
    return with_funding


# Loop through and grab weekday/Sat/Sun subsets of joined data, calculate runtimes
def create_service_estimator_data(list_of_dates: list):    

    
    combined_trip_times = pd.concat(
        [pd.read_parquet(f"{DATA_PATH}trip_run_times_{d}.parquet") 
         for d in list_of_dates], axis=0
    )
    
    combind_shape_frequency = combined_trip_times.pipe(frequency_by_shape)
    
    
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
    

def clip_shapes(
    shapes: dg.GeoDataFrame, 
    tracts_by_category: gpd.GeoDataFrame, 
    category: str
) -> dd.DataFrame:

    clipped = dg.clip(shapes, tracts_by_category.loc[[category]])
    clipped = clipped.assign(
        tract_type = category,
        pct_category = clipped.geometry.length / clipped.total_length
    ).drop(columns = ["geometry", "total_length"])

    return clipped
    
        
def generate_shape_categories(selected_date: str) -> gpd.GeoDataFrame:
    
    shape_cols = ["feed_key", "shape_id"]
    
    shapes = helpers.import_scheduled_shapes(
        selected_date,
        columns = shape_cols + ["geometry"],
        get_pandas = False,
        crs = geography_utils.CA_NAD83Albers
    )
    
    shapes = shapes.assign(
        total_length = shapes.geometry.length
    )
    
    census_tracts = (catalog.calenviroscreen_lehd_by_tract.read()
                     .to_crs(geography_utils.CA_NAD83Albers)
                     [["Tract", "pop_sq_mi", "geometry"]]
             )

    census_tracts = census_tracts.assign(
        tract_type = census_tracts.pop_sq_mi.apply(
            lambda x: 'urban' if x > 2_400 
            else 'suburban' if x > 800 
            else 'rural'),
        ## quick fix for invalid geometries (comes up in dissolve later)
        geometry = census_tracts.geometry.buffer(0),
    )
    
    tracts_by_category = census_tracts[
        ["tract_type", "geometry"]
    ].dissolve(by='tract_type')

    tract_categories = ["urban", "suburban", "rural"]

    results = dd.multi.concat(
        [clip_shapes(shapes, tracts_by_category, c)
         for c in tract_categories],
        axis=0
    ).repartition(npartitions=1)
    
    shapes2 = dd.merge(
        shapes,
        results,
        on = shape_cols,
        how = "inner"
    ).compute()
    
    shapes3 = shapes2.assign(
        pct_max = (shapes2.groupby(shape_cols)
                   .pct_category
                   .transform("max"))
    ).query(
        'pct_max == pct_category'
    ).drop(columns = ["pct_category", "pct_max"])
    
    utils.geoparquet_gcs_export(
        shapes3, 
        DATA_PATH, 
        f'shapes_categorized_{selected_date}'
    ) 
    
    
#------------------------------------------------------------------#
## Functions to create tract-level dataset
#------------------------------------------------------------------#
def create_bus_arrivals_by_tract_data(selected_date: str):
    aggregated_stops_with_geom = pd.read_parquet(
        f"{utils.DATA_PATH}aggregated_stops_with_geom_{selected_date}.parquet")

    census_tracts = catalog.calenviroscreen_lehd_by_tract.read()
    
    gdf = gpd.sjoin(
        aggregated_stops_with_geom, 
        census_tracts.to_crs(geography_utils.WGS84),
        # Use inner, or else left join will result in some NaN tracts
        how = "inner",
        predicate = "intersects"
    ).drop(columns = "index_right")
    
    # Aggregate by tract level and count bus arrivals, number of stops, etc
    gdf2 = portfolio_utils.aggregate_by_geography(
        gdf, 
        group_cols = ["Tract"], 
        sum_cols = ["num_arrivals"],
        count_cols = ["stop_id"],
        nunique_cols = ["schedule_gtfs_dataset_key"]
    )
    
    # Attach tract geometry back, since our previous spatial join kept bus stop's point geometry
    final = pd.merge(
        census_tracts,
        gdf2,
        on = "Tract",
        how = "left"
    )
    
    # Export to GCS
    utils.geoparquet_gcs_export(
        final, 
        utils.GCS_FILE_PATH, 
        "bus_stop_times_by_tract"
    )


if __name__ == "__main__":
    # Run this to get the static parquet files
    
    # Get analysis dataset for service increase estimator?
    df = pd.concat(
        [pd.read_parquet(
            f"{DATA_PATH}trip_run_times_{d}.parquet")
        for d in analysis_date_list], axis=0
    ).pipe(frequency_by_shape)
    
    create_service_estimator_data()
    generate_shape_categories(analysis_date)
    
    # Get analysis dataset for bus arrivals by tract
    create_bus_arrivals_by_tract_data(analysis_date)