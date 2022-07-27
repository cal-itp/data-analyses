"""
Clean up the combined rail/ferry/BRT points
and get it ready to be combined with other bus-related points.

From combine_and_visualize.ipynb
"""
import dask_geopandas
from utilities import catalog_filepath

COMPILED_RAIL_BRT_FERRY = catalog_filepath("rail_brt_ferry_initial")
   

def get_rail_ferry_brt_extract():
    df = dask_geopandas.read_parquet(COMPILED_RAIL_BRT_FERRY)

    keep_cols = ["calitp_itp_id", "stop_id", 
                "route_type", "geometry"]
                            
    df2 = (df[keep_cols].assign(
            hqta_type = df.route_type.map(
                lambda x: "major_stop_rail" if x in ["0", "1", "2"]
                else "major_stop_brt" if x == "3" 
                else "major_stop_ferry")
        ).rename(columns = {"calitp_itp_id": "calitp_itp_id_primary"})
       .drop(columns = "route_type")
    )

    return df2 