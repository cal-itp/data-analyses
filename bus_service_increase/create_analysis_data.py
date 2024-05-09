"""
Create analysis data for service increase estimator
and tract-level stats.
"""
import dask.dataframe as dd
import datetime
import geopandas as gpd
import intake
import pandas as pd

from dask import delayed, compute

from calitp_data_analysis import geography_utils, utils
from segment_speed_utils import helpers
from shared_utils import portfolio_utils

catalog = intake.open_catalog("*.yml")

#------------------------------------------------------------------#
## Functions to create operator-route-level dataset
#------------------------------------------------------------------#
def calculate_frequency(
    df: pd.DataFrame,
    group_cols: list = [
        "schedule_gtfs_dataset_key", "name", "day_name", 
        "departure_hour", "shape_id", "route_id"]
) -> pd.DataFrame:
    """
    Aggregate by route_id-shape_id-departure_hour-day_name
    and count how many trips occurred and the average service_minutes.
    """
    trips_per_hour = (df
                      .groupby(group_cols, 
                              observed=True, group_keys=False)
                      .agg({
                          "trip_instance_key": "count",
                          "scheduled_service_minutes": "mean"
                      })
                      .reset_index()
                      .rename(columns = {
                          "trip_instance_key": "n_trips",
                          "scheduled_service_minutes": "avg_service_minutes"
                      }).astype({"n_trips": "int32"})
                     )

    trips_per_hour2 = (trips_per_hour
                       .drop_duplicates(group_cols)
                       .sort_values(group_cols)
                      ).reset_index(drop=True)
    
    return trips_per_hour2


def expand_rows_fill_with_zeros(
    df: pd.DataFrame,
    group_cols: list = [
        "schedule_gtfs_dataset_key", 
        "day_name", "departure_hour",
        "route_id", "shape_id"
    ]
) -> pd.DataFrame:
    """
    Use group_cols to uniquely identify a row that we want to expand 
    and fill in rows that don't have service with zeros. 
    We will use operator-day_name-hour-route-shape.
    """    
    # Set the iterables to be exact order as shape_cols
    iterables = [
        df.schedule_gtfs_dataset_key.unique(),
        df.day_name.unique(),
        range(0, 25), # set this to be all hours
        df.route_id.unique(),
        df.shape_id.unique(),
    ]
    
    multi_ix = pd.MultiIndex.from_product(
        iterables, 
        names = group_cols
    )
    
    df2 = df.set_index(group_cols)
    df2 = df2[~df2.index.duplicated(keep="first")]
    
    df_expanded = (df2.reindex(multi_ix)
                   .reset_index()
                  )
    
    # Fill with zeroes
    df_expanded = df_expanded.assign(
        n_trips = df_expanded.n_trips.fillna(0).astype("int32"),
    ).astype({
        "departure_hour": "int8"
    })
    
    return df_expanded
    

def clip_shapes(
    shapes: gpd.GeoDataFrame, 
    dissolved_tracts: gpd.GeoDataFrame, 
    category: str
) -> pd.DataFrame:
    """
    Census tracts are dissolved by categories. 
    Do a clipping and calculate the length of each shape that falls
    into an urban/suburban/rural tract.
    """
    clipped = gpd.clip(
        shapes[["shape_array_key", "total_length", "geometry"]], 
        dissolved_tracts.loc[[category]]
    )
    
    clipped = clipped.assign(
        pct_category = round(clipped.geometry.length / clipped.total_length, 3)
    )[["shape_array_key", "pct_category"]].rename(
        columns = {"pct_category": f"pct_{category}"})

    return clipped


def get_shapes(selected_date: str) -> gpd.GeoDataFrame:
    """
    Get shapes and calculate length of shape in meters.
    """
    # Add schedule gtfs_dataset_key to use instead of feed_key
    natural_ids = helpers.import_scheduled_trips(
        selected_date,
        columns = ["gtfs_dataset_key", "name", 
                   "shape_array_key", "shape_id"],
        get_pandas = True
    ).drop_duplicates()
    
    shapes = helpers.import_scheduled_shapes(
        selected_date,
        columns = ["shape_array_key", "geometry"],
        get_pandas = True,
        crs = geography_utils.CA_NAD83Albers
    ).pipe(
        helpers.remove_shapes_outside_ca
    ).merge(
        natural_ids,
        on = "shape_array_key",
        how = "inner"
    ).dropna(subset="geometry")
    
    shapes = shapes.assign(
        total_length = shapes.geometry.length
    )
    
    return shapes    
     
    
def dissolve_census_tracts(
    crs: str = geography_utils.CA_NAD83Albers
) -> gpd.GeoDataFrame:
    census_tracts = (
        catalog.calenviroscreen_lehd_by_tract.read()
        .to_crs(crs)
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
    
    return tracts_by_category   


def generate_shape_categories(date_list: list):
    """
    Concatenate shapes file for list of dates.
    Dissolve census tracts by urban / suburban / rural.
    Categorize shapes into one of those census tract categories
    based on plurality.
    """
    shapes = delayed(pd.concat)([
        get_shapes(d) for d in date_list], 
        axis=0, ignore_index=True
    ).drop_duplicates(subset="shape_array_key")
    
    # Dissolve census tracts by urban / suburban / rural
    census_tracts = dissolve_census_tracts()

    # Clip shape by census tract and calculate 
    # what percent of shape falls into urban / suburban / rual
    urban_clip = delayed(clip_shapes)(shapes, census_tracts, "urban") 
    suburban_clip = delayed(clip_shapes)(shapes, census_tracts, "suburban") 
    rural_clip = delayed(clip_shapes)(shapes, census_tracts, "rural") 
    
    clip_results = delayed(pd.merge)(
        urban_clip,
        suburban_clip,
        on = "shape_array_key",
        how = "left"
    ).merge(
        rural_clip, 
        on = "shape_array_key", 
        how = "left"
    )
    
    results = compute(clip_results)[0]

    # Find the name of the column (idxmax, axis=1) that has the max value
    # and then replace the string in there to show urban instead of pct_urban
    #https://stackoverflow.com/questions/29919306/find-the-column-name-which-has-the-maximum-value-for-each-row
    results = results.assign(
        tract_type = results[["pct_urban", 
                              "pct_suburban", "pct_rural"]
                            ].idxmax(axis=1).str.replace("pct_", "")
    )[["shape_array_key", "tract_type"]]

    shapes = compute(shapes)[0]
    
    # we grab 3 dates, just in case shape_array_keys change
    # in this time period
    # so we want to drop dupes and keep all possible shape_array_keys
    shapes_categorized = pd.merge(
        shapes,
        results,
        on = "shape_array_key",
        how = "left"
    ).drop_duplicates().reset_index(drop=True).rename(
        columns = {"total_length": "total_length_meters"})     
    
    utils.geoparquet_gcs_export(
        shapes_categorized, 
        DATA_PATH, 
        "shapes_categorized"
    ) 
    
    
#------------------------------------------------------------------#
## Functions to create tract-level dataset
#------------------------------------------------------------------#
def create_bus_arrivals_by_tract_data(selected_date: str):
    """
    Aggregate bus arrivals to tract.
    """
    aggregated_stops_with_geom = gpd.read_parquet(
        f"{DATA_PATH}aggregated_stops_with_geom_{selected_date}.parquet")

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
        sum_cols = ["n_arrivals"],
        count_cols = ["stop_id"],
        nunique_cols = ["schedule_gtfs_dataset_key"],
    ).rename(columns = {
        "n_arrivals": "total_arrivals",
        "stop_id": "n_stops",
        "schedule_gtfs_dataset_key": "n_operators"
    })
    
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
        DATA_PATH, 
        f"bus_stop_times_by_tract_{selected_date}"
    )

    
if __name__ == "__main__":
    
    from service_increase_vars import dates, DATA_PATH
    
    start = datetime.datetime.now()
    
    # Run this to get the static parquet files    
    all_dates = list(dates.values())
    
    df = delayed(pd.concat)([
            pd.read_parquet(
                f"{DATA_PATH}trip_run_times_{d}.parquet"
            ) for d in all_dates
        ], axis=0, ignore_index=True
    )

    shape_cols = [
        "schedule_gtfs_dataset_key", "name", "day_name", 
        "departure_hour", 
        "shape_id", "route_id"
    ]

    frequency_by_route = delayed(calculate_frequency)(
        df,
        group_cols = shape_cols
    )
    
    time1 = datetime.datetime.now()
    print(f"get frequency by route: {time1 - start}")
    
    frequency_by_route = compute(frequency_by_route)[0]
    frequency_by_route.to_parquet(f"{DATA_PATH}shape_frequency.parquet")
    
    del frequency_by_route
    
    operators = pd.read_parquet(
        f"{DATA_PATH}trip_run_times_{dates['wed']}.parquet",
        columns = ["schedule_gtfs_dataset_key"]
    ).schedule_gtfs_dataset_key.unique()
    
    
    frequency_dfs = [
        delayed(pd.read_parquet)(
           f"{DATA_PATH}shape_frequency.parquet",
           filters = [[("schedule_gtfs_dataset_key", "==", one_operator)]]
    ) for one_operator in operators]
    
    expanded_dfs = [
        delayed(expand_rows_fill_with_zeros)(one_df) 
        for one_df in frequency_dfs
    ]
    
    results_ddf = dd.from_delayed(expanded_dfs) 
    
    SHAPES_PROCESSED_FILE = "shapes_processed"
    
    # Partitioned parquet as a release valve since we're 
    # running close to memory
    results_ddf.to_parquet(
        f"{DATA_PATH}{SHAPES_PROCESSED_FILE}", 
        partition_on = "schedule_gtfs_dataset_key",
        overwrite=True
    )
    
    shapes_processed = pd.read_parquet(
        f"{DATA_PATH}{SHAPES_PROCESSED_FILE}/"
    )
    
    shapes_processed.to_parquet(f"{DATA_PATH}{SHAPES_PROCESSED_FILE}.parquet")

    helpers.if_exists_then_delete(f"{DATA_PATH}{SHAPES_PROCESSED_FILE}/")
    print("delete partitioned")
    del shapes_processed

    time2 = datetime.datetime.now()
    print(f"save expanded df: {time2 - time1}")
    
    generate_shape_categories(all_dates)

    time3 = datetime.datetime.now()
    print(f"shape categories: {time3 - time2}")

    # Get analysis dataset for bus arrivals by tract
    #create_bus_arrivals_by_tract_data(dates['wed'])
    
