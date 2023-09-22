"""
Do linear referencing by segment-trip 
and derive speed.
"""
import os
os.environ['USE_PYGEOS'] = '0'

import dask.dataframe as dd
import dask_geopandas as dg
import datetime
import geopandas as gpd
import numpy as np
import pandas as pd
import sys

from loguru import logger

from shared_utils import geography_utils 
from segment_speed_utils import helpers, segment_calcs, wrangle_shapes
from segment_speed_utils.project_vars import (SEGMENT_GCS, analysis_date, 
                                              PROJECT_CRS, CONFIG_PATH)    
from shared_utils.rt_utils import MPH_PER_MPS


def linear_referencing_vp_against_line(
    vp: dd.DataFrame, 
    segments: gpd.GeoDataFrame,
    segment_identifier_cols: list,
    timestamp_col: str
) -> dd.DataFrame:
    """
    Take the vp x,y columns, make into gdf.
    Merge in segment geometry and do linear referencing.
    Return just the shape_meters result and timestamp converted to seconds.
    """
    time0 = datetime.datetime.now()
    
    # https://stackoverflow.com/questions/71685387/faster-methods-to-create-geodataframe-from-a-dask-or-pandas-dataframe
    # https://github.com/geopandas/dask-geopandas/issues/197    
    vp_gddf = dg.from_dask_dataframe(
        vp, 
        geometry=dg.points_from_xy(vp, "x", "y")
    ).set_crs(geography_utils.WGS84).to_crs(PROJECT_CRS).drop(columns = ["x", "y"])
    
    vp_with_seg_geom = dd.merge(
        vp_gddf, 
        segments,
        on = segment_identifier_cols,
        how = "inner"
    ).rename(columns = {
        "geometry_x": "vp_geometry",
        "geometry_y": "segment_geometry"}
    ).set_geometry("vp_geometry")

    vp_with_seg_geom = vp_with_seg_geom.repartition(npartitions=50)
          
    time1 = datetime.datetime.now()
    logger.info(f"set up merged vp with segments: {time1 - time0}")
    
    shape_meters_series = vp_with_seg_geom.map_partitions(
        wrangle_shapes.project_point_geom_onto_linestring,
        "segment_geometry",
        "vp_geometry",
        meta = ("shape_meters", "float")
    )
    
    vp_with_seg_geom = segment_calcs.convert_timestamp_to_seconds(
        vp_with_seg_geom, [timestamp_col])
    
    vp_with_seg_geom = vp_with_seg_geom.assign(
        shape_meters = shape_meters_series,
        segment_meters = vp_with_seg_geom.segment_geometry.length
    )
    
    time2 = datetime.datetime.now()
    logger.info(f"linear referencing: {time2 - time1}")
    
    drop_cols = [f"{timestamp_col}", "vp_geometry", "segment_geometry"]
    vp_with_seg_geom2 = vp_with_seg_geom.drop(columns = drop_cols)
    
    return vp_with_seg_geom2
    

def make_wide_get_speed(
    df: dd.DataFrame, 
    group_cols: list,
    timestamp_col: str
) -> dd.DataFrame:
    """
    Get df wide and set up current vp_idx and get meters/sec_elapsed
    against prior and calculate speed.
    """
    vp2 = (
        df.groupby(group_cols, 
                   observed=True, group_keys=False)
        .agg({"vp_idx": "max"})
        .reset_index()
        .merge(
            df,
            on = group_cols + ["vp_idx"],
            how = "inner"
        )
    )

    vp1 = (
        df.groupby(group_cols, 
                   observed=True, group_keys=False)
        .agg({"vp_idx": "min"})
        .reset_index()
        .merge(
            df,
            on = group_cols + ["vp_idx"],
            how = "inner"
        ).rename(columns = {
            "vp_idx": "prior_vp_idx",
            f"{timestamp_col}_sec": f"prior_{timestamp_col}_sec",
            "shape_meters": "prior_shape_meters",
        })
    )
    
    df_wide = dd.merge(
        vp2,
        vp1,
        on = group_cols,
        how = "left"
    )
    
    speed = segment_calcs.derive_speed(
        df_wide,
        distance_cols = ("prior_shape_meters", "shape_meters"),
        time_cols = (f"prior_{timestamp_col}_sec", f"{timestamp_col}_sec")
    )
    
    speed = speed.assign(
        pct_segment = speed.meters_elapsed.divide(speed.segment_meters)
    )
    
    return speed

   
def filter_for_unstable_speeds(
    df: pd.DataFrame,
    pct_segment_threshold: float
) -> tuple[pd.DataFrame]:
    ok_speeds = df[df.pct_segment > pct_segment_threshold]
    low_speeds = df[df.pct_segment <= pct_segment_threshold]
    
    return ok_speeds, low_speeds


def recalculate_low_speeds_with_straight_distance(
    low_speeds_df: pd.DataFrame,
    group_cols: list,
    timestamp_col: str
):
    """
    For low speed segments, select a different vp_idx.
    Use the current vp_idx and subtract by 1.
    This will fill in something where the segment only had 1 point previously.
    """
    keep_cols = group_cols + [
        "vp_idx", "location_timestamp_local_sec",
    ]
    
    df1 = low_speeds_df[keep_cols].drop_duplicates().reset_index(drop=True)
    
    df1 = df1.assign(
        prior_vp_idx = df1.vp_idx - 1
    )
    
    usable_vp = dd.read_parquet(
        f"{SEGMENT_GCS}vp_usable_{analysis_date}",
        columns = ["trip_instance_key",
                   "vp_idx", timestamp_col, "x", "y"]
    )

    vp_idx_bounds = segment_calcs.get_usable_vp_bounds_by_trip(usable_vp)
    
    df2 = pd.merge(
        df1, 
        vp_idx_bounds,
        on = "trip_instance_key",
        how = "inner"
    )
    
    # Check that the prior_vp_idx actually is on the same trip (must be within bounds)
    # If not, select the next point
    df2 = df2.assign(
        prior_vp_idx = df2.apply(
            lambda x: 
            x.vp_idx + 1 if (x.prior_vp_idx < x.min_vp_idx) and 
            (x.vp_idx + 1 <= x.max_vp_idx)
            else x.prior_vp_idx, 
            axis=1)
    ).drop(columns = ["trip_instance_key", "min_vp_idx", "max_vp_idx"])
    
    # We will need point geom again, since we are using straight distance
    subset_vp_idx = np.union1d(
        df2.vp_idx.unique(), 
        df2.prior_vp_idx.unique()
    ).tolist()
    
    usable_vp2 = usable_vp[usable_vp.vp_idx.isin(subset_vp_idx)].compute()
    
    usable_gdf = geography_utils.create_point_geometry(
        usable_vp2,
        longitude_col = "x",
        latitude_col = "y",
        crs = PROJECT_CRS
    ).drop(columns = ["x", "y"]).reset_index(drop=True)
    
    usable_gdf2 = segment_calcs.convert_timestamp_to_seconds(
        usable_gdf, [timestamp_col]).drop(columns = timestamp_col)
    
    # Merge in coord for current_vp_idx
    # we already have a timestamp_sec for current vp_idx
    gdf = pd.merge(
        usable_gdf2.drop(columns = f"{timestamp_col}_sec"),
        df2,
        on = "vp_idx",
        how = "inner"
    )
    
    # Merge in coord for prior_vp_idx
    gdf2 = pd.merge(
        gdf,
        usable_gdf2[
            ["vp_idx", f"{timestamp_col}_sec", "geometry"]
        ].add_prefix("prior_"),
        on = "prior_vp_idx",
        how = "inner"
    )
    
    # should we do straight distance or interpolate against full shape?
    # what if full shape is problematic?
    # do we want to do a check against the scale? that's not very robust either though

    gdf2 = gdf2.assign(
        straight_distance = gdf2.geometry.distance(gdf2.prior_geometry)
    )
    
    gdf2 = gdf2.assign(
        sec_elapsed = (gdf2[f"{timestamp_col}_sec"] - 
                   gdf2[f"prior_{timestamp_col}_sec"]).abs()
    )
    
    gdf2 = gdf2.assign(
        speed_mph = gdf2.straight_distance.divide(gdf2.sec_elapsed) * MPH_PER_MPS
    )
    
    drop_cols = ["geometry", "prior_geometry"]
    results = gdf2.drop(columns = drop_cols)
                        
    return results


def linear_referencing_and_speed_by_segment(
    analysis_date: str,
    dict_inputs: dict = {}
):
    """
    With just enter / exit points on segments, 
    do the linear referencing to get shape_meters, and then derive speed.
    Do a second pass for low speed segments with straight distance.
    """
    time0 = datetime.datetime.now()    
    
    VP_FILE = dict_inputs["stage3"]
    SEGMENT_FILE = dict_inputs["segments_file"]
    SEGMENT_IDENTIFIER_COLS = dict_inputs["segment_identifier_cols"]
    TIMESTAMP_COL = dict_inputs["timestamp_col"]    
    EXPORT_FILE = dict_inputs["stage4"]
    PCT_SEGMENT_MIN = dict_inputs["pct_segment_minimum"]
    
    # Keep subset of columns - don't need it all. we can get the 
    # columns dropped through segments file
    vp_keep_cols = [
        'trip_instance_key',
        TIMESTAMP_COL,
        'x', 'y', 'vp_idx'
    ] + SEGMENT_IDENTIFIER_COLS
    
    vp = dd.read_parquet(
        f"{SEGMENT_GCS}{VP_FILE}_{analysis_date}",
        columns = vp_keep_cols
    )
    
    segments = helpers.import_segments(
        SEGMENT_GCS,
        f"{SEGMENT_FILE}_{analysis_date}", 
        columns = SEGMENT_IDENTIFIER_COLS + ["geometry"]
    ).dropna(subset="geometry").reset_index(drop=True)
     
    vp_with_seg_geom = linear_referencing_vp_against_line(
        vp,
        segments,
        SEGMENT_IDENTIFIER_COLS,
        TIMESTAMP_COL
    ).persist()

    time1 = datetime.datetime.now()
    logger.info(f"linear referencing: {time1 - time0}")
    
    SEGMENT_TRIP_COLS = ["trip_instance_key", 
                         "segment_meters"] + SEGMENT_IDENTIFIER_COLS

    initial_speeds = make_wide_get_speed(
        vp_with_seg_geom, SEGMENT_TRIP_COLS, TIMESTAMP_COL
    ).compute()
    
    
    time2 = datetime.datetime.now()
    logger.info(f"make wide and get initial speeds: {time2 - time1}")
    
    ok_speeds, low_speeds = filter_for_unstable_speeds(
        initial_speeds,
        pct_segment_threshold = PCT_SEGMENT_MIN
    )
    
    low_speeds_recalculated = recalculate_low_speeds_with_straight_distance(
        low_speeds,
        SEGMENT_TRIP_COLS,
        TIMESTAMP_COL
    )
    
    # Add a flag that tells us speed was recalculated
    # Combine columns and rename straight distance as meters_elapsed
    low_speeds_recalculated = low_speeds_recalculated.assign(
        flag_recalculated = 1,
        meters_elapsed = low_speeds_recalculated.straight_distance
    )
        
    keep_cols = SEGMENT_TRIP_COLS + [
        "vp_idx", "prior_vp_idx", 
        f"{TIMESTAMP_COL}_sec", f"prior_{TIMESTAMP_COL}_sec",
        "meters_elapsed",
        "sec_elapsed",
        "pct_segment",
        "speed_mph",
        "flag_recalculated",
    ]
    
    speeds = pd.concat([
        ok_speeds,
        low_speeds_recalculated
    ], axis=0).sort_values(SEGMENT_IDENTIFIER_COLS + ["trip_instance_key"]
                      ).reset_index(drop=True)    
    
    speeds = speeds.assign(
        flag_recalculated = speeds.flag_recalculated.fillna(0).astype("int8")
    )[keep_cols]
    
    time3 = datetime.datetime.now()
    logger.info(f"recalculate speeds and get final: {time3 - time2}")
    
    speeds.to_parquet(
        f"{SEGMENT_GCS}{EXPORT_FILE}_{analysis_date}.parquet", 
    )
    
    
if __name__ == "__main__": 
    
    LOG_FILE = "../logs/speeds_by_segment_trip.log"
    logger.add(LOG_FILE, retention="3 months")
    logger.add(sys.stderr, 
               format="{time:YYYY-MM-DD at HH:mm:ss} | {level} | {message}", 
               level="INFO")
    
    logger.info(f"Analysis date: {analysis_date}")
    
    start = datetime.datetime.now()

    STOP_SEG_DICT = helpers.get_parameters(CONFIG_PATH, "stop_segments")
        
    linear_referencing_and_speed_by_segment(analysis_date, STOP_SEG_DICT)
            
    logger.info(f"speeds for stop segments: {datetime.datetime.now() - start}")
    logger.info(f"execution time: {datetime.datetime.now() - start}")
            