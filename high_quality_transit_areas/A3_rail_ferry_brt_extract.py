"""
Clean up the combined rail/ferry/BRT points
and get it ready to be combined with other bus-related points.

From combine_and_visualize.ipynb
"""
import dask_geopandas as dg
from utilities import catalog_filepath
from A2_combine_stops import new_muni_stops

COMPILED_RAIL_BRT_FERRY = catalog_filepath("rail_brt_ferry_initial")
   

def get_rail_ferry_brt_extract():
    df = dg.read_parquet(COMPILED_RAIL_BRT_FERRY)

    keep_cols = ["calitp_itp_id", "stop_id", 
                "route_type", "geometry"]
    
    # Temporarily assign route_type for new Muni stops to be rail
    # since route_info wasn't attached 
    df = df.assign(
        route_type = df.apply(
            lambda x: "1" if x.stop_id in new_muni_stops and 
            x.calitp_itp_id==282
            else x.route_type, axis=1, meta=('route_type', 'str'))
    )
        
    df2 = (df[keep_cols].assign(
            hqta_type = df.route_type.map(
                lambda x: "major_stop_rail" if x in ["0", "1", "2"]
                else "major_stop_brt" if x == "3" 
                else "major_stop_ferry")
        ).rename(columns = {"calitp_itp_id": "calitp_itp_id_primary"})
       .drop(columns = "route_type")
    )

    return df2 