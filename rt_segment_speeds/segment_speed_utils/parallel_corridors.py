"""
Utility functions for working with
State Highway Network and transit routes.

Highways Raw data: [SHN on Geoportal](https://opendata.arcgis.com/datasets/77f2d7ba94e040a78bfbe36feb6279da_0.geojson) 
Processed data: run raw data through `create_parallel_corridors.py` 
export to GCS, put in catalog.

Transit Routes: Need a shapes level table with route info.
Usually, this can be achieved by merging `trips` and `shapes`.
"""
import geopandas as gpd
import gcsfs
import pandas as pd

from calitp_data_analysis import geography_utils, utils
from segment_speed_utils import gtfs_schedule_wrangling, helpers
from shared_utils import catalog_utils

BUS_SERVICE_GCS = "gs://calitp-analytics-data/data-analyses/bus_service_increase/"
fs = gcsfs.GCSFileSystem()
hwy_group_cols = ["Route", "County", "District", "RouteType"]

def process_transit_routes(analysis_date: str) -> gpd.GeoDataFrame:
    """
    For each route, select the longest shape for each route
    to overlay with SHN.
    Also count how many routes there are for each operator.
    """
    longest_shape = gtfs_schedule_wrangling.longest_shape_by_route_direction(
        analysis_date
    ).pipe(helpers.remove_shapes_outside_ca)
    
    gdf = longest_shape.assign(
        total_routes = longest_shape.groupby("feed_key").route_key.transform("nunique")
    ).sort_values(
        ["route_key", "route_length"], 
        ascending = [True, False]
    ).drop_duplicates(subset="route_key").reset_index(drop=True)
    
    # Get this to same CRS as highways
    gdf = gdf.assign(
        route_length_feet = gdf.geometry.to_crs(geography_utils.CA_StatePlane).length
    ).drop(columns = "route_length").to_crs(geography_utils.CA_StatePlane)
    
    
    return gdf


def process_highways(
    group_cols: list,
    buffer_feet: int
) -> gpd.GeoDataFrame:
    """
    Put in a list of group_cols, and aggregate highway segments with 
    the direction info up to the group_col level.
    
    For each highway, store what directions it runs in 
    as dummy variables. This will allow us to dissolve 
    the geometry and get fewer rows for highways
    without losing direction info.
    """
    SHN_FILE = catalog_utils.get_catalog("shared_data_catalog").state_highway_network.urlpath
    
    direction_cols = ["NB", "SB", "EB", "WB"]

    df = (gpd.read_parquet(SHN_FILE)
          .to_crs(geography_utils.CA_StatePlane)
         )
    
    # Get dummies for direction
    # Can make data wide instead of long
    direction_dummies = pd.get_dummies(df.Direction, dtype=int)
    df = pd.concat([df.drop(columns = "Direction"), 
                    direction_dummies], axis=1)
    
    # For each highway, allow multiple dummies to be 1 (as long as highway had that direction,
    # we'll allow dummy to be 1. A highway can be tagged as WB and SB, and we want to keep info for both).
    for c in direction_cols:
        df[c] = df.groupby(group_cols)[c].transform('max').astype(int)
    
    # Buffer first, then dissolve
    # If dissolve first, then buffer, kernel times out
    df = df.assign(
        highway_feet = df.geometry.length,
        geometry = df.geometry.buffer(buffer_feet),
        Route = df.Route.astype(int),
    )
    
    df2 = df.dissolve(by=group_cols + direction_cols).reset_index()
    
    df2[direction_cols] = df2[direction_cols].astype(int)
        
    return df2


def overlay_transit_to_highways(
    analysis_date: str,
    hwy_buffer_feet: int,
    pct_route_threshold: float,
) -> gpd.GeoDataFrame:
    """
    Function to find areas of intersection between
    highways (default of 1 mile buffer) and transit routes.
    
    Returns: geopandas.GeoDataFrame, with geometry column reflecting
    the areas of intersection.
    """    
    # Can pass a different buffer zone to determine parallel corridors
    HWY_FILE = f"{BUS_SERVICE_GCS}highways_buffer{hwy_buffer_feet}.parquet"
    
    if fs.exists(HWY_FILE):
        highways = gpd.read_parquet(HWY_FILE)
    else:
        highways = process_highways(
            group_cols = hwy_group_cols, 
            buffer_feet = hwy_buffer_feet
        )
    transit_routes = process_transit_routes(analysis_date)
    
    # Overlay
    # Note: an overlay based on intersection changes the geometry column
    # The new geometry column will reflect that area of intersection
    gdf = gpd.overlay(
        transit_routes, 
        highways, 
        how = "intersection", 
        keep_geom_type = True
    )  
    
    # Using new geometry column, calculate what % that intersection 
    # is of the route and hwy
    gdf = gdf.assign(
        pct_route = (gdf.geometry.length / gdf.route_length_feet).round(3),
        Route = gdf.Route.astype(int),
    )
    
    # Fix geometry - don't want the overlay geometry, which is intersection
    # Want to use a transit route's line geometry    
    gdf2 = pd.merge(
        transit_routes[["route_key", "geometry"]],
        gdf.drop(columns = "geometry"),
        on = "route_key",
        how = "inner",
        # Allow 1:m merge because the same transit route 
        # can overlap with various highways
        validate = "1:m",
    )
    
    gdf3 = gdf2.loc[gdf2.pct_route >= pct_route_threshold]
    
    return gdf3


def routes_by_on_shn_parallel_categories(
    analysis_date: str
) -> gpd.GeoDataFrame:
    """
    Categorize routes into on_shn / parallel / other.
    Only unique routes are kept (route_key).
    """
    # Tweak these if needed - for now, on SHN is defined as being within 50 ft of SHN,
    # and parallel to SHN is within 0.5 mile of SHN.
    # since these are concentric circle buffers, we'll allow a route to be assigned
    # to a category based on ranking (shn > parallel > other).
    # A route (longest shape) can overlay with multiple highways, but we're looking across all highways
    # and if the route qualifies as on_shn for any of the highways its near, it is on_shn.
    SHN_HWY_BUFFER_FEET = 50 
    SHN_PCT_ROUTE = 0.2 # we'll use the same for both categories
    PARALLEL_HWY_BUFFER_FEET = geography_utils.FEET_PER_MI * 0.5
    
    transit_routes = process_transit_routes(analysis_date)

    on_shn = overlay_transit_to_highways(
        analysis_date,
        hwy_buffer_feet = SHN_HWY_BUFFER_FEET,
        pct_route_threshold = SHN_PCT_ROUTE,
    )[["route_key"]].drop_duplicates()
    
    parallel_or_intersecting = overlay_transit_to_highways(
        analysis_date,
        hwy_buffer_feet = PARALLEL_HWY_BUFFER_FEET, 
        pct_route_threshold = SHN_PCT_ROUTE,
    )[["route_key"]].drop_duplicates()
    
    df = pd.merge(
        transit_routes.assign(other=1),
        on_shn.assign(on_shn=3),
        on = "route_key",
        how = "left"
    ).merge(
        parallel_or_intersecting.assign(parallel=2),
        on = "route_key",
        how = "left"
    )
    
    #https://stackoverflow.com/questions/29919306/find-the-column-name-which-has-the-maximum-value-for-each-row
    category_cols = ["on_shn", "parallel", "other"]
    keep_cols = [
        "feed_key", "schedule_gtfs_dataset_key",
        "route_id", "route_key",
        "total_routes", "category",
        "geometry",
        "route_length_feet"
    ]
    
    df2 = df.assign(
        category = df[category_cols].idxmax(axis=1)
    )[keep_cols]

    return df2


if __name__ == "__main__":
        
    SHN_HWY_BUFFER_FEET = 50 
    PARALLEL_HWY_BUFFER_FEET = int(geography_utils.FEET_PER_MI * 0.5)    
    
    highways_shn_buffer = process_highways(
        group_cols = hwy_group_cols, 
        buffer_feet = SHN_HWY_BUFFER_FEET
    )
    
    utils.geoparquet_gcs_export(
        highways_shn_buffer,
        BUS_SERVICE_GCS,
        f"highways_buffer{SHN_HWY_BUFFER_FEET}"
    )
    
    print(f"exported highways_buffer{SHN_HWY_BUFFER_FEET}")
    del highways_shn_buffer
    
    highways_parallel_buffer = process_highways(
        group_cols = hwy_group_cols, 
        buffer_feet = PARALLEL_HWY_BUFFER_FEET
    )
    
    utils.geoparquet_gcs_export(
        highways_parallel_buffer,
        BUS_SERVICE_GCS,
        f"highways_buffer{PARALLEL_HWY_BUFFER_FEET}"
    )
    
    print(f"exported highways_buffer{PARALLEL_HWY_BUFFER_FEET}")