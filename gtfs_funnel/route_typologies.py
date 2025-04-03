"""
For route typologies for GTFS routes,
use a combination of GTFS info and NACTO road info to categorize into typologies.
Capture the typology each year for route_id, since this is fairly static.

* start with route grain (trips)
* categorize based on route_type or route_name
  - rail, ferry categorized by route_type values
  - express, rapid categorized by route_short_name / route_long_name
  - everything else is local bus
* local buses have additional opportunity to get new 
typologies based on NACTO definitions (service frequencies on roads). 
grab 1 shape for each route.
  - downtown_local
  - local
  - coverage
  - rapid 
* use spatial join and keep top 2 categories for each shape 
* only top 2 NACTO typologies are assigned
* between the route typologies and NACTO typologies, for local buses,
if any of the dummy variables are 1, use that.

~ roughly 2 min per year
"""
import datetime
import geopandas as gpd
import pandas as pd

from segment_speed_utils import gtfs_schedule_wrangling, helpers, time_series_utils               
from segment_speed_utils.project_vars import PROJECT_CRS   
from update_vars import SHARED_GCS, SCHED_GCS, COMPILED_CACHED_VIEWS, GTFS_DATA_DICT
import nacto_utils

route_cols = ["schedule_gtfs_dataset_key", "route_id"]

typology_cols = ["freq_category", "typology"]
road_cols = ["linearid", "mtfcc", "fullname"]
road_segment_cols = road_cols + ["segment_sequence"]

def categorize_route_types(
    date_list: list
) -> pd.DataFrame:
    """
    Concatenate schedule metrics (from gtfs_funnel)
    for route-direction-time_period grain
    for all the dates we have.
    """
    FILE = GTFS_DATA_DICT.schedule_downloads.trips
        
    df = time_series_utils.concatenate_datasets_across_dates(
        COMPILED_CACHED_VIEWS,
        FILE,
        date_list,
        data_type = "df",
        columns = [
            "gtfs_dataset_key", "name",
            "route_id", "route_type",
            "route_long_name", "route_short_name"
        ],
    ).drop(
        columns = "service_date"
    ).rename(
        columns = {"gtfs_dataset_key": "schedule_gtfs_dataset_key"}
    ).drop_duplicates().reset_index(drop=True)
    
    df = df.assign(
        route_id = df.route_id.fillna(""),
        route_short_name = df.route_short_name.fillna(""),
        route_long_name = df.route_long_name.fillna(""),
    )

    df = df.assign(
        combined_name = df.route_short_name + "__" + df.route_long_name
    )
    
    typology_tags = df.apply(
        lambda x: nacto_utils.tag_rapid_express_rail_ferry(
            x.combined_name, x.route_type), axis=1
    )
    
    df2 = pd.concat([df, typology_tags], axis=1)

    df2 = df2.assign(
        is_local = df2.apply(
            lambda x: 
            1 if (x.is_express==0) and (x.is_rapid==0) and 
            (x.is_rail==0) and (x.is_ferry==0)
            else 0, axis=1).astype(int)
    )
    
    return df2

def prep_roads(year: str, buffer_meters: int, dict_inputs: dict) -> gpd.GeoDataFrame:
    """
    Uses aggregated sjoin to count stop arrivals on roads
    from stop_arrivals_in_roads.py.
    Assigns service frequency to road segment (across operators).
    """
    road_stats = pd.read_parquet(
        f"{SCHED_GCS}arrivals_by_road_segment_{str(year)}.parquet"
    )

    ROAD_SEGMENTS = dict_inputs.shared_data.road_segments_twomile
    
    roads = gpd.read_parquet(
        f"{SHARED_GCS}{ROAD_SEGMENTS}.parquet",
        columns = road_segment_cols + ["geometry"],
    ).to_crs(PROJECT_CRS)
    
    road_stats = road_stats.assign(
        freq_category = road_stats.apply(
            lambda x: nacto_utils.nacto_peak_frequency_category(x.frequency), axis=1)
    )
    
    road_stats = road_stats.assign(
        typology = road_stats.apply(
            lambda x: nacto_utils.nacto_stop_frequency(
            x.stops_per_mi, x.freq_category), axis=1)
    )
    
    df = pd.merge(
        roads,
        road_stats,
        on = road_segment_cols,
        how = "inner"
    )
    
    # Get the length of each road segment and buffer the roads by some amount,
    # in preparation of a spatial join
    df = df.assign(
        road_meters = df.geometry.length,
        geometry = df.geometry.buffer(buffer_meters)
    )
        
    return df

def get_local_buses_attach_shape(
    bus_routes: pd.DataFrame, 
    date_list: list
):
    """
    These bus routes have an opportunity to get 
    differentiated categories, instead of local buses.
    Can be downtown_local, local, rapid (if frequency and stop spacing is met),
    or coverage.
    """
    subset_routes = bus_routes.route_id.unique().tolist()
    subset_operators = bus_routes.schedule_gtfs_dataset_key.unique().tolist()
    
    # Grab the most common shape for route-direction, then dedupe to keep route only
    common_shape = pd.concat([
        gtfs_schedule_wrangling.most_common_shape_by_route_direction(
            analysis_date,
            trip_filters = [[
                ("route_id", "in", subset_routes),
                ("gtfs_dataset_key", "in", subset_operators)
            ]]
        )
        for analysis_date in date_list
    ], axis=0, ignore_index=True).sort_values(
        route_cols + ["direction_id"]
    ).drop(
        columns = "direction_id"
    ).drop_duplicates(
        subset = route_cols
    ).reset_index(drop=True)
    
    # Attach the common shape_id geometry for each route_id
    bus_routes_with_shape = pd.merge(
        common_shape,
        bus_routes,
        on = route_cols,
        how = "inner"
    )
    return bus_routes_with_shape


def spatial_join_to_roads_and_categorize(
    bus_routes: gpd.GeoDataFrame,
    roads: gpd.GeoDataFrame,
) -> gpd.GeoDataFrame:
    """
    Do spatial join between 
    one shape per route with buffered roads and 
    roughly count up the road segment lengths
    for each NACTO-categorized roads.
    This roughly approximates how much of the transit shape falls 
    along certain types of roads.
    
    Used to do overlay, but this was fairly time-consuming for just
    keeping the primary/secondary typologies anyway.
    """
    bus_routes_to_roads = gpd.sjoin(
        bus_routes,
        roads,
        how = "inner",
        predicate = "intersects"
    )
    
    # aggregate and count road_meters 
    categorized = (bus_routes_to_roads
            .groupby(route_cols + ["freq_category", "typology"], 
                     group_keys=False, dropna=False)
            .agg({"road_meters": "sum"})
            .reset_index()
           )

    # Only keep the largest 2 NACTO road categories, use rank ordering
    primary_secondary = categorized.assign(
        ranking = (categorized.sort_values(route_cols + ["road_meters"],
                                      ascending=[True for c in route_cols] + [False])
                   .groupby(route_cols, 
                            group_keys=False, dropna=False)
                   .cumcount() + 1
                  )
    ).query('ranking <= 2')
    
    # Turn the typology column into a dummy variables
    primary_secondary = pd.get_dummies(primary_secondary, columns = ["typology"])

    # Flag both primary and secondary typology as 1
    # so a route can have multiple dummies turned on
    # allow this so we can just keep one route-dir as a row
    max_cols = [c for c in primary_secondary.columns if "typology_" in c]

    primary_secondary_dummies = (primary_secondary.groupby(route_cols)
           .agg({**{c: "max" for c in max_cols}})
           .reset_index()
           .rename(columns = {c: c.replace('typology', 'is_nacto') for c in max_cols})
          )
    
    # Merge NACTO dummy categories on, and use left merge because there will be some that 
    # only have route type categorization but not NACTO
    # This is ok, we'll fill in dummies with zeroes
    # Overall, we're going to pick the most favorable category anyway
    bus_routes_typologized = pd.merge(
        bus_routes.drop(columns = "geometry"),
        primary_secondary_dummies,
        on = route_cols,
        how = "left",
    ).fillna(
        {**{c: 0 for c in primary_secondary_dummies.columns if "is_nacto" in c}}
    ).pipe(nacto_utils.reconcile_route_and_nacto_typologies)

    return bus_routes_typologized


if __name__ == "__main__":
    
    from shared_utils import rt_dates
    
    EXPORT = GTFS_DATA_DICT.schedule_tables.route_typologies
    
    start = datetime.datetime.now()
 
    year = rt_dates.current_year    
    analysis_date_list = rt_dates.DATES_BY_YEAR_DICT[year]
        
    ROAD_BUFFER_METERS = 20
    roads = prep_roads(year, ROAD_BUFFER_METERS, GTFS_DATA_DICT)

    # Grab routes from GTFS scheduled trips for all cached dates this year
    routes = categorize_route_types(analysis_date_list)

    # Separate out bus routes that might get more differentiated
    # typologies based on NACTO definitions
    bus_routes = routes[
        (routes.route_type == "3") & 
        (routes.is_local == 1)
    ].reset_index(drop=True)

    # Find 1 shape per route
    bus_routes = get_local_buses_attach_shape(
        bus_routes, analysis_date_list
    )

    bus_routes_typologies = spatial_join_to_roads_and_categorize(
        bus_routes, roads)


    # Concatenate results between local buses (which gets extra sjoin)
    # and non-bus which uses straight GTFS route_type categorization
    non_bus_routes = routes[
        (routes.route_type != "3") | 
        (routes.is_rapid == 1) | 
        (routes.is_express == 1)
    ].reset_index(drop=True)

    route_typology_df = pd.concat(
        [non_bus_routes, bus_routes_typologies], 
        axis=0, ignore_index=True
    ).sort_values(route_cols).reset_index(drop=True)

    route_typology_df.to_parquet(f"{SCHED_GCS}{EXPORT}_{year}.parquet")
        
    end = datetime.datetime.now()
    print(f"route typologies for {year}: {end - start}")
