"""
Create analysis data for service increase estimator
and tract-level stats.
"""
import dask.dataframe as dd
import dask_geopandas as dg
import geopandas as gpd
import intake
import pandas as pd

from dask import delayed

from calitp_data_analysis import geography_utils, utils
from segment_speed_utils import helpers
from shared_utils import portfolio_utils

catalog = intake.open_catalog("*.yml")

#------------------------------------------------------------------#
## Functions to create operator-route-level dataset
#------------------------------------------------------------------#
def frequency_by_shape(df: pd.DataFrame) -> pd.DataFrame:
    """
    Aggregate by route_id-shape_id-departure_hour-day_name
    and count how many trips occurred and the average service_minutes.
    """
    group_cols = ["schedule_gtfs_dataset_key", 
                  "day_name", "departure_hour", 
                  "shape_id"]
    
    trips_per_hour = (df
                      .groupby(group_cols + ["route_id"], 
                              observed=True, group_keys=False)
                      .agg({
                          "trip_instance_key": "count",
                          "service_minutes": "mean"
                      })
                      .reset_index()
                      .rename(columns = {
                          "trip_instance_key": "n_trips",
                          "service_minutes": "avg_service_minutes"
                      }).astype({"n_trips": "int32"})
                     )

    # Should we deal with multiple route_ids that share the same shape_id?
    # Used to...but does that still happen in v2?
    # For now, we'll do a basic drop duplicates
    trips_per_hour2 = (trips_per_hour
                       .sort_values(group_cols + ["route_id"])
                       .drop_duplicates(group_cols)
                      ).reset_index(drop=True)
    
    return trips_per_hour2

    
@delayed
def get_shape_frequency(list_of_dates, filters: tuple):
    df = pd.concat(
        [pd.read_parquet(
            f"{DATA_PATH}trip_run_times_{d}.parquet",
            filters = filters,
        ).pipe(frequency_by_shape) for d in list_of_dates])
    
    return df


@delayed
def expand_rows_fill_with_zeros(df: pd.DataFrame):
    # This is what we want to use to uniquely identify a shape_id-departure_hour
    # since shape_id can easily be shared across operators (ie shape_id == 1 or A)
    shape_cols = [
        "schedule_gtfs_dataset_key", 
        "day_name", "departure_hour",
        "route_id", "shape_id"
    ]
    
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
        names = shape_cols
    )
    
    df2 = df.set_index(shape_cols)
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
    shapes: dg.GeoDataFrame, 
    tracts_by_category: gpd.GeoDataFrame, 
    category: str
) -> dd.DataFrame:
    """
    Census tracts are dissolved by categories. 
    Do a clipping and calculate the length of each shape that falls
    into an urban/suburban/rural tract.
    """
    clipped = dg.clip(shapes, tracts_by_category.loc[[category]])
    clipped = clipped.assign(
        tract_type = category,
        pct_category = clipped.geometry.length / clipped.total_length
    ).drop(columns = ["geometry", "total_length"])

    return clipped
    
        
def generate_shape_categories(selected_date: str) -> gpd.GeoDataFrame:
    """
    For each shape, categorize it into urban/suburban/rural based
    on plurality of lengths. Each shape has a portion that falls in
    urban, suburban, or rural tracts, and the portion that is the longest
    is what the shape is categorized as.
    """
    shape_cols = ["feed_key", "shape_id"]
    
    # Add schedule gtfs_dataset_key to use instead of feed_key
    feed_dataset_key_crosswalk = helpers.import_scheduled_trips(
        selected_date,
        columns = ["gtfs_dataset_key", "feed_key"],
        get_pandas = True
    ).drop_duplicates()
    
    shapes = helpers.import_scheduled_shapes(
        selected_date,
        columns = shape_cols + ["geometry"],
        get_pandas = False,
        crs = geography_utils.CA_NAD83Albers
    ).merge(
        feed_dataset_key_crosswalk,
        on = "feed_key",
        how = "inner"
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
        on = ["schedule_gtfs_dataset_key"] + shape_cols,
        how = "inner"
    ).compute()
    
    shapes3 = shapes2.assign(
        pct_max = (shapes2.groupby(["schedule_gtfs_dataset_key"] + 
                                   shape_cols, 
                                   observed=True, group_keys=False)
                   .pct_category
                   .transform("max"))
    ).query(
        'pct_max == pct_category'
    ).drop(columns = ["pct_category", "pct_max", "total_length"])
    
    
    utils.geoparquet_gcs_export(
        shapes3, 
        DATA_PATH, 
        f'shapes_categorized_{selected_date}'
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
    
    # Run this to get the static parquet files    
    all_dates = list(dates.values())
    
    # Get analysis dataset for service increase estimator?
    operators = pd.read_parquet(
        f"{DATA_PATH}trip_run_times_{dates['wed']}.parquet",
        columns = ["schedule_gtfs_dataset_key"]
    ).schedule_gtfs_dataset_key.unique()
    
    frequency_dfs = [
        get_shape_frequency(
            all_dates, 
            filters = [[
                ("schedule_gtfs_dataset_key", "==", one_operator)
            ]]) 
        for one_operator in operators]

    expanded_dfs = [
        expand_rows_fill_with_zeros(one_df) 
        for one_df in frequency_dfs
    ]
    
    results_ddf = dd.from_delayed(expanded_dfs)    
    results_ddf.to_parquet(
        f"{DATA_PATH}shapes_processed", 
        partition_on = "schedule_gtfs_dataset_key"
    )
    
    results = pd.read_parquet(
        f"{DATA_PATH}shapes_processed/"
    )
    
    # Partitioned parquet as a release valve since we're 
    # running close to memory
    results.to_parquet(f"{DATA_PATH}shapes_processed.parquet")
    print("save parquet")
    
    helpers.if_exists_then_delete(f"{DATA_PATH}shapes_processed/")
    print("delete partitioned")
    
    generate_shape_categories(dates['wed'])
    
    # Get analysis dataset for bus arrivals by tract
    create_bus_arrivals_by_tract_data(dates['wed'])
