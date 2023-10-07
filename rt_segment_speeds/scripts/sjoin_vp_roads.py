import dask.dataframe as dd
import dask_geopandas as dg
import datetime
import geopandas as gpd
import pandas as pd

import A1_sjoin_vp_segments as A1
from calitp_data_analysis.geography_utils import WGS84
from segment_speed_utils import helpers, segment_calcs
from segment_speed_utils.project_vars import (analysis_date, SEGMENT_GCS, 
                                              CONFIG_PATH, PROJECT_CRS)
from shared_utils import rt_utils


def get_vp_direction(vp: dg.GeoDataFrame) -> dg.GeoDataFrame:
    usable_bounds = segment_calcs.get_usable_vp_bounds_by_trip(vp)

    vp_gddf = vp.assign(
        prior_vp_idx = vp.vp_idx - 1
    ).merge(
        usable_bounds, 
        on = "trip_instance_key",
        how = "inner"
    )
    
    unknown_direction = vp_gddf[vp_gddf.vp_idx == vp_gddf.min_vp_idx]     
    can_get_direction = vp_gddf[vp_gddf.vp_idx != vp_gddf.min_vp_idx]
    
    # Dask gdf does not like to to renaming on-the-fly
    vp_gddf_renamed = (vp_gddf[["vp_idx", "geometry"]]
                   .add_prefix("prior_")
                   .set_geometry("prior_geometry")
                  )
    
    vp_with_prior = dd.merge(
        can_get_direction,
        vp_gddf_renamed,
        on = "prior_vp_idx",
        how = "inner"
    )
    
    vp_with_prior["vp_primary_direction"] = vp_with_prior.apply(
        lambda x: 
        rt_utils.primary_cardinal_direction(x.prior_geometry, x.geometry),
        axis=1, meta = ("vp_primary_direction", "object")
    )
    
    
    can_get_direction_results = vp_with_prior[["vp_idx", "vp_primary_direction"]]
    unknown_direction_results = unknown_direction.assign(vp_primary_direction="Unknown")
    
    vp_direction = dd.multi.concat(
        [can_get_direction_results, 
         unknown_direction_results], axis=0
    ).sort_values("vp_idx").reset_index(drop=True)
    
    return vp_direction
     

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
    vp = dd.read_parquet(
        f"{SEGMENT_GCS}{INPUT_FILE}_{analysis_date}/",
        columns = ["trip_instance_key", "vp_idx", "x", "y"],
    )
    
    vp_gddf = dg.from_dask_dataframe(
        vp,
        geometry = dg.points_from_xy(vp, x="x", y="y", crs=WGS84)
    ).set_crs(WGS84).to_crs(PROJECT_CRS).drop(columns = ["x", "y"])
    
    vp_gddf = vp_gddf.repartition(npartitions=100)
    
    time1 = datetime.datetime.now()
    print(f"prep vp and persist: {time1 - time0}")
    
    # save dtypes as a dict to input in map_partitions
    seg_id_dtypes = segments[SEGMENT_IDENTIFIER_COLS].dtypes.to_dict()
    
    vp_direction = get_vp_direction(vp_gddf)
    
    vp_gddf2 = dd.merge(
        vp_gddf,
        vp_direction,
        on = "vp_idx",
        how = "inner"
    ).persist()
    
    def subset_by_direction(
        vp: dg.GeoDataFrame,
        segments: dg.GeoDataFrame,
        direction: str
    ) -> tuple[dg.GeoDataFrame]:
        direction_list = [direction, "Unknown"]
        subset_vp = vp[vp.vp_primary_direction.isin(direction_list)]
        subset_segments = segments[segments.primary_direction.isin(direction_list)]
        
        return subset_vp, subset_segments
        
        
    vp_north, segments_north = subset_by_direction(vp_gddf2, segments, "Northbound")
    vp_south, segments_south = subset_by_direction(vp_gddf2, segments, "Southbound")
    
    results_north = get_sjoin_results(
        vp_north,
        segments_north,
        SEGMENT_IDENTIFIER_COLS,
    )
    
        
    results_south = get_sjoin_results(
        vp_south,
        segments_south,
        SEGMENT_IDENTIFIER_COLS,
    )
    
    time2 = datetime.datetime.now()
    print(f"sjoin with map_partitions: {time2 - time1}")
    
    # An sjoin like this will give 13_524_111 results...13M combinations is too much
    # Bring in direction earlier
    results = dd.multi.concat(
        [results_north, results_south], axis=0
    ).reset_index(drop=True).repartition(npartitions=5)
    
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