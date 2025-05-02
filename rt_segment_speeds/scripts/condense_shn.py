"""
Use output from `cut_road_segments.py` and condense this
into 1 km cutoff points for roads.

Store results once.
"""
import datetime
import geopandas as gpd
import pandas as pd

from calitp_data_analysis import utils

from segment_speed_utils import vp_transform
from segment_speed_utils.project_vars import GTFS_DATA_DICT, PROJECT_CRS, SHARED_GCS

    
if __name__ == "__main__":
    
    start = datetime.datetime.now()
    
    road_segment_cols = [*GTFS_DATA_DICT.shared_data.road_segment_cols]
    segment_type = "onekm"
    
    shn = gpd.read_parquet(
        f"{SHARED_GCS}segmented_roads_{segment_type}_2020.parquet",
        columns = road_segment_cols + ["destination"],
    ).to_crs(PROJECT_CRS).pipe(
        vp_transform.condense_point_geom_to_line,
        group_cols = ["linearid", "fullname"],
        geom_col = "destination",
        array_cols = ["segment_sequence"],
        sort_cols = ["linearid", "fullname", "segment_sequence"]
    ).rename(
        columns = {"destination": "geometry"}
    ).set_geometry(
        "geometry"
    ).set_crs(
        PROJECT_CRS
    )
        
    utils.geoparquet_gcs_export(
        shn,
        SHARED_GCS,
        f"condensed_shn_{segment_type}"
    )
    
    end = datetime.datetime.now()
    print(f"condense {segment_type} SHN segments: {end - start}")
    
    #condense onekm SHN segments: 0:00:33.344454    