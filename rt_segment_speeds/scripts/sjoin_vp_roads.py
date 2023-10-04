import dask.dataframe as dd
import dask_geopandas as dg
import datetime
import geopandas as gpd
import pandas as pd

import A1_sjoin_vp_segments as A1
from calitp_data_analysis.geography_utils import WGS84
from segment_speed_utils import helpers
from segment_speed_utils.project_vars import (analysis_date, SEGMENT_GCS, 
                                              CONFIG_PATH, PROJECT_CRS)

def get_sjoin_results(
    vp_gddf: dg.GeoDataFrame, 
    segments: gpd.GeoDataFrame, 
    segment_identifier_cols: list,
) -> pd.DataFrame:
    """
    Merge all the segments for a shape for that trip,
    and check if vp is within.
    Export just vp_idx and seg_idx as our "crosswalk" of sjoin results.
    If we use dask map_partitions, this is still faster than dask.delayed.
    """
    vp_to_seg = dg.sjoin(
        vp_gddf,
        segments,
        how = "inner",
        predicate = "within"
    )[["vp_idx"] + segment_identifier_cols]
    
    results = (vp_to_seg
               .drop_duplicates()
               .reset_index(drop=True)
              )
    
    return results

def sjoin_vp_to_segments(
    analysis_date: str,
    dict_inputs: dict = {}
):
    """
    Spatial join vehicle positions to segments.
    Subset by grouping columns.
    
    Vehicle positions can only join to the relevant segments.
    Use route_dir_identifier or shape_array_key to figure out 
    the relevant segments those vp can be joined to.
    """
    INPUT_FILE = dict_inputs["stage1"]
    SEGMENT_FILE = dict_inputs["segments_file"]
    TRIP_GROUPING_COLS = dict_inputs["trip_grouping_cols"]
    GROUPING_COL = dict_inputs["grouping_col"]
    SEGMENT_IDENTIFIER_COLS = dict_inputs["segment_identifier_cols"]
    EXPORT_FILE = dict_inputs["stage2"]
    
    BUFFER_METERS = 35
    
    time0 = datetime.datetime.now()
    
    segments = A1.import_segments_and_buffer(
        f"{SEGMENT_FILE}_{analysis_date}",
        BUFFER_METERS,
        SEGMENT_IDENTIFIER_COLS,
        filters = [[("mtfcc", "in", ["S1100", "S1200"])]]
    )
    
    # Import vp, keep trips that are usable
    vp = helpers.import_vehicle_positions(
        SEGMENT_GCS,
        f"{INPUT_FILE}_{analysis_date}/",
        columns = ["trip_instance_key", 
                   "vp_idx", "x", "y"],
        partitioned = True
    )
    
    vp_gddf = dg.from_dask_dataframe(
        vp,
        geometry = dg.points_from_xy(vp, x="x", y="y", crs=WGS84)
    ).set_crs(WGS84).to_crs(PROJECT_CRS).drop(columns = ["x", "y"])
    
    vp_gddf = vp_gddf.repartition(npartitions=100).persist()
    
    time1 = datetime.datetime.now()
    print(f"prep vp and persist: {time1 - time0}")
    
    # save dtypes as a dict to input in map_partitions
    seg_id_dtypes = segments[SEGMENT_IDENTIFIER_COLS].dtypes.to_dict()
    
    results = get_sjoin_results(
        vp_gddf,
        segments,
        SEGMENT_IDENTIFIER_COLS,
        #meta = {"vp_idx": "int64", **seg_id_dtypes},
        #align_dataframes = False
    )
    
    time2 = datetime.datetime.now()
    print(f"sjoin with map_partitions: {time2 - time1}")
    
    # An sjoin like this will give 13_524_111 results...13M combinations is too much
    # Bring in direction earlier
    results = results.repartition(npartitions=5)
    results.to_parquet(
        f"{SEGMENT_GCS}vp_sjoin/{EXPORT_FILE}_{analysis_date}",
        overwrite=True
    )
    
    time3 = datetime.datetime.now()
    print(f"export partitioned results: {time3 - time2}")
    
    
if __name__ == "__main__":
    ROAD_SEG_DICT = helpers.get_parameters(CONFIG_PATH, "road_segments")

    sjoin_vp_to_segments(
        analysis_date = analysis_date,
        dict_inputs = ROAD_SEG_DICT
    )