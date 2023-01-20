"""
Clean up the combined rail/ferry/BRT points
and get it ready to be combined with other bus-related points.

From combine_and_visualize.ipynb
"""
import geopandas as gpd
import intake

catalog = intake.open_catalog("*.yml")
   
def get_rail_ferry_brt_extract():
    """
    Prepare the rail / ferry / BRT stops to be assembled with
    the bus_hqta types and saved into the hqta_points file.
    """
    df = catalog.rail_brt_ferry_initial.read()

    keep_cols = ["feed_key", "name", "stop_id", 
                 "route_type", "geometry"]
    
    rail_types = ["0", "1", "2"]
    bus_types = ["3"]
    ferry_types = ["4"]
    
    df2 = (df[keep_cols].assign(
            hqta_type = df.route_type.map(
                lambda x: "major_stop_rail" if x in rail_types
                else "major_stop_brt" if x in bus_types
                else "major_stop_ferry" if x in ferry_types 
                else "missing" # add flag to make it easier to check results
            )
        ).rename(columns = {"name": "name_primary", 
                            "feed_key": "feed_key_primary"})
       .drop(columns = "route_type")
    )

    return df2 