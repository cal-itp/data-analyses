"""
Find local buses that run same origin / destination
as Amtrak thruway buses.
"""
import dask.dataframe as dd
import dask_geopandas as dg
import geopandas as gpd
import pandas as pd

from shapely.geometry import Point

from _utils import GCS_FILE_PATH, SELECTED_DATE, COMPILED_CACHED_VIEWS
from shared_utils import geography_utils


def keep_long_shape_ids(routelines: dg.GeoDataFrame | gpd.GeoDataFrame, 
                        mile_cutoff: float | int = 20
                       ) -> dg.GeoDataFrame | gpd.GeoDataFrame:
    """
    Filter down routelines file to just routes that are pretty long
    with shape_id.
    """
    routelines = routelines.to_crs(geography_utils.CA_StatePlane)
    
    routelines = routelines.assign(
        route_mi = routelines.geometry.length.divide(
            geography_utils.FEET_PER_MI).round(2)
    )
    
    long_routes = (routelines[routelines.route_mi >= mile_cutoff]
                   .drop_duplicates()
                  )
        
    return long_routes


def keep_longest_route(routelines: gpd.GeoDataFrame | dg.GeoDataFrame, 
                       trips: pd.DataFrame | dd.DataFrame
                      ) -> gpd.GeoDataFrame:
    """
    Merge in trip table, which has route_id.
    Find the shape_id that is the longest to stand-in for the route_id.
    
    Also add an origin / destination to that shape_id.
    """    
    # Filter trips dataset down too
    route_cols = ["calitp_itp_id", "route_id", 
                  "route_short_name", "route_long_name", "route_desc",
                  "route_type", "shape_id"]
    
    trips_route_info = (trips[(trips.route_type == '3')]
                        [route_cols]
                        .drop_duplicates(
                            subset=["calitp_itp_id", "route_id", "shape_id"])
                        .reset_index(drop=True)
                       )   
    
    # Don't need to use gtfs_build.merge_routes_trips
    # here because goal is not to get trip-level data with line geom.
    # Instead, want to know which one is the longest shape_id
    # for that route_id
    m1 = dd.merge(
        routelines,
        trips_route_info,
        on = ["calitp_itp_id", "shape_id"],
        how = "inner"
    )
    
    # Find the route that is the longest
    # Do this now to quickly filter down df before generating new columns that
    # might be time-consuming, and compute needs to be called already
    max_route_mi = (m1.groupby(["calitp_itp_id", "route_id"])
                    .route_mi.max().reset_index())
    
    m2 = dd.merge(
        m1, 
        max_route_mi,
        on = ["calitp_itp_id", "route_id", "route_mi"],
        how = "inner"
    ).compute() # compute now because dask can't sort by multiple columns
    
    keep_cols = ["calitp_itp_id", "route_id", "route_mi", 
                 "route_short_name", "route_long_name", 
                 "route_type", "shape_id", 
                 "geometry", 
                ]
    
    # If there are still multiple shape_ids with same max_route_mi, deal with here
    longest_route = (m2.sort_values(["calitp_itp_id", "route_id", "route_mi"],
                                  ascending = [True, True, False])
                     .drop_duplicates(subset = ["calitp_itp_id", "route_id"])
                     .reset_index(drop=True)
                      [keep_cols]
                    )
    
    # Add the route's origin and destination
    longest_route = longest_route.assign(
        origin = longest_route.geometry.apply(lambda x: Point(x.coords[0])),
        destination = longest_route.geometry.apply(lambda x: Point(x.coords[-1])),
    )
    
    return longest_route


def buffer_around_origin_destination(gdf: gpd.GeoDataFrame, 
                                     buffer_feet: int = 0) -> gpd.GeoDataFrame:
    """
    Since we have origin / destination point geom,
    might want to draw a 5 mile, 10 mile buffer around the origin?
    Somehow capture local bus routes that also travel
    to the same cities, but not necessarily stop at the same train station.
    """
    # Project to CA State Plane (feet)
    # for all geom cols, just keep for standardization
    geom_cols = list(gdf.select_dtypes("geometry").columns)
    
    for c in geom_cols:
        gdf[c] = gdf[c].to_crs(geography_utils.CA_StatePlane)
    
    gdf = gdf.assign(
        origin_buffer = gdf.origin.buffer(buffer_feet),
        destination_buffer = gdf.destination.buffer(buffer_feet)
    )
    
    return gdf


def reshape_wide_to_long_and_dissolve(
    gdf: gpd.GeoDataFrame, 
    id_vars: list,
    value_vars: list,
    var_name: str = "variable",
    value_name: str = "value"
) -> gpd.GeoDataFrame:
    """
    Since origin / destinations are in separate columns, 
    change this to long, so a dissolve can be done.
    
    Take all the inputs normally for pd.melt().
    
    Returns a long gdf that's been dissolved by id_vars.
    """

    gdf_long = pd.melt(
        gdf[id_vars + value_vars],
        id_vars = id_vars,
        value_vars = value_vars,
        var_name = var_name,
        value_name = value_name
    )
    
    dissolved = (gdf_long.dissolve(by = id_vars)
                .reset_index()
                .drop(columns = var_name)
    )
    
    # In the melting, we lose the geometry column that was set
    # Even though it's still gdf, need to reset the CRS
    dissolved.crs = gdf.crs 
    dissolved = dissolved.set_geometry(value_name)
    
    return dissolved

if __name__ == "__main__":
    trips = dd.read_parquet(
        f"{COMPILED_CACHED_VIEWS}trips_{SELECTED_DATE}.parquet"
    )

    routelines = dg.read_parquet(
        f"{COMPILED_CACHED_VIEWS}routelines_{SELECTED_DATE}.parquet")
    
    # Filter down to at least 20 mile long shape_ids
    routelines2 = keep_long_shape_ids(routelines, mile_cutoff=20)
    
    # Of these, merge these shape_ids to trips and get trip_id and route_id
    longest_route = keep_longest_route(routelines2, trips)
    
     # Take route-level Amtrak thruway routes
    amtrak_routes = catalog.amtrak_thruway_routes_with_od.read()
    
    # Draw a 5 mile buffer around origin / destination
    amtrak_routes2 = buffer_around_origin_destination(
        amtrak_routes[amtrak_routes.route_type=='3'], 
        buffer_feet = geography_utils.FEET_PER_MI * 5)
    
    amtrak_od = reshape_wide_to_long_and_dissolve(
        amtrak_routes2, 
        id_vars = ["calitp_itp_id", "route_id", "origin_destination"],
        value_vars = ["origin_buffer", "destination_buffer"],
        value_name = "geometry"
    )
    
    longest_route_diss = reshape_wide_to_long_and_dissolve(
        longest_route,
        id_vars = ["calitp_itp_id", "route_id"],
        value_vars = ["origin", "destination"],
        value_name = "geometry"
    )
    
    intersects_amtrak = gpd.sjoin(
        longest_route_diss, 
        # only need to set origin as geometry, since amtrak_od contains both OD
        amtrak_od[["geometry"]], 
        how = "inner",
        predicate = "within"
    )[["calitp_itp_id", "route_id", "geometry"]].drop_duplicates()
    
    routes_intersect_amtrak = pd.merge(
        longest_route,
        intersects_amtrak.drop(columns = "geometry"),
        on = ["calitp_itp_id", "route_id"],
        how = "inner",
        validate = "1:1"
    )
    
    routes_intersect_amtrak.to_parquet("routes_intersect_amtrak.parquet")