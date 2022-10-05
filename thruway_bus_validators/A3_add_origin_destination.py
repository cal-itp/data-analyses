"""
For Amtrak thruway buses, even the same route can have
slightly different origin/destination.

Add origin_destination as a column, which accounts for 
both directions.

Create a df at the route-origin_destination level, which
associated origin / destination point geom, line geom to
be used to find local buses.
"""
import geopandas as gpd
import intake
import pandas as pd

import _utils
from shared_utils import utils

catalog = intake.open_catalog("*.yml")


def create_od_pairs(gdf: gpd.GeoDataFrame) -> list[str, str]:
    """
    Return a list of possible origin / destination pairs.
    
    Returns in this format: 
       [ ['los_angeles', 'bakersfield'],
         ['sacramento', 'san_jose'] ]
    """
    od_pairs = []

    origin_stops = gdf.origin_stop_name.unique().tolist()
    
    for o in sorted(origin_stops):
        # First, find if origin / destination has this stop name
        subset = gdf[(gdf.origin_stop_name==o) | 
                     (gdf.destination_stop_name==o)]

        # Find all the possible combos that do occur, and get the set() of that
        other_origins = subset.origin_stop_name.unique().tolist()
        other_destinations = subset.destination_stop_name.unique().tolist()

        unique_combo = list(set(other_origins + other_destinations))

        # Remove this stop_name
        possible_pairs = [i for i in unique_combo if i != o]

        # Take this list and combine with this origin stop name
        for i in possible_pairs:
            # If the flipped version doesn't already exist, add it
            # Ex: if LA-Sacramento doesn't already exist, ok to add Sacramento-LA
            if [i, o] not in od_pairs: 
                od_pairs.append([o, i])
                
    return od_pairs


def add_origin_destination_column(gdf: gpd.GeoDataFrame, 
                                  od_pairs: list) -> pd.DataFrame: 
    """
    Add a column that tracks origin-destination.
    Stores same value for LA-Bakersfield and Bakersfield-LA.
    """
    with_od = pd.DataFrame()

    keep_cols = ["calitp_itp_id", "route_id", 
                 "origin_stop_name", "destination_stop_name"]
    
    for i in od_pairs:
        subset = (gdf[(
            ((gdf.origin_stop_name==i[0]) & (gdf.destination_stop_name==i[1])) |
            ((gdf.origin_stop_name==i[1]) & (gdf.destination_stop_name==i[0]))
        )][keep_cols]
         .drop_duplicates()
        )

        # Add this combo
        subset = subset.assign(
            origin_destination = i[0] + "-" + i[1]
        )
    
        # Concatenante the ressults and compare against original gdf
        # If length differs too much, need to examine why, 
        # but it's ok to be miss the ones where OD is within same location
        with_od = pd.concat([with_od, subset], 
                            axis=0).drop_duplicates()
        
    return with_od


if __name__ == "__main__":
    trip_gdf = catalog.amtrak_thruway_trips.read()
    
    gdf = _utils.aggregate_to_route(trip_gdf)
    
    gdf = gdf.assign(
        origin_stop_name = (gdf.origin_stop_name.str.replace(' ', '_')
                            .str.lower()),
        destination_stop_name = (gdf.destination_stop_name.str.replace(' ', '_')
                                 .str.lower()),
    )
    
    od_pairs = create_od_pairs(gdf)
    
    df = add_origin_destination_column(gdf, od_pairs)
    
    gdf2 = pd.merge(
        gdf, 
        df,
        on = ["calitp_itp_id", "route_id", 
              "origin_stop_name", "destination_stop_name"],
        how = "left",
        validate = "m:1",
    )
    
    utils.geoparquet_gcs_export(
        gdf2,
        _utils.GCS_FILE_PATH,
        "amtrak_thruway_routes_with_od"
    )
    
    