import datetime
import geopandas as gpd
import numpy as np
import pandas as pd
import shapely

from calitp_data_analysis import utils
from calitp_data_analysis.geography_utils import WGS84
from segment_speed_utils import helpers, wrangle_shapes
from segment_speed_utils.project_vars import SEGMENT_GCS, GTFS_DATA_DICT, PROJECT_CRS
from shared_utils import rt_dates

analysis_date = rt_dates.DATES["apr2024"]

def merge_nearest_vp_with_shape(analysis_date: str):
    vp = gpd.read_parquet(
        f"{SEGMENT_GCS}nearest/"
        f"test_nearest_vp_to_stop_{analysis_date}.parquet",
    )
    
    shapes = helpers.import_scheduled_shapes(
        analysis_date,
        columns = ["shape_array_key", "geometry"],
        filters = [[("shape_array_key", "in", vp.shape_array_key.unique())]],
        crs = PROJECT_CRS,
        get_pandas = True
    )
    
    vp_with_shape = pd.merge(
        vp,
        shapes.rename(columns = {"geometry": "shape_geometry"}),
        on = "shape_array_key",
        how = "inner"
    )
    
    return vp_with_shape


def explode_vp_and_project_onto_shape(
    vp_with_shape: gpd.GeoDataFrame,
    analysis_date: str
) -> gpd.GeoDataFrame:
    vp_long = vp_with_shape.explode(
        "nearest_vp_arr"
    ).reset_index(drop=True).rename(
        columns = {"nearest_vp_arr": "vp_idx"}
    )
    
    subset_vp = vp_long.vp_idx.unique().tolist()

    vp_with_dwell = pd.read_parquet(
        f"{SEGMENT_GCS}vp_usable_with_dwell_{analysis_date}",
        filters = [[("vp_idx", "in", subset_vp)]],
        columns = ["vp_idx", "x", "y", "location_timestamp_local", 
                   "moving_timestamp_local"]
    ).pipe(wrangle_shapes.vp_as_gdf, crs = PROJECT_CRS)
    
    gdf = pd.merge(
        vp_long, 
        vp_with_dwell.rename(columns = {"geometry": "vp_geometry"}),
        on = "vp_idx",
        how = "inner"
    )
    
    gdf = gdf.assign(
        stop_meters = gdf.shape_geometry.project(gdf.stop_geometry),
        shape_meters = gdf.shape_geometry.project(gdf.vp_geometry)
    )
    
    gdf = gdf.assign(
        stop_vp_distance_meters = (gdf.stop_meters - 
                                   gdf.shape_meters).round(2)
    )
    
    return gdf


def find_two_closest_vp(gdf: gpd.GeoDataFrame, group_cols: list):

    positive_distances_df = gdf.loc[gdf.stop_vp_distance_meters >= 0]
    negative_distances_df = gdf.loc[gdf.stop_vp_distance_meters < 0]
    
    #https://github.com/pandas-dev/pandas/issues/45089
    # add dropna=False or else too many combos are lost
    min_pos_distance = (
        positive_distances_df
        .groupby(group_cols, 
                 observed=True, group_keys=False, dropna=False)
        .agg({"stop_vp_distance_meters": "min"})
        .reset_index()
    )
    
    min_neg_distance = (
        negative_distances_df
        .groupby(group_cols, 
                 observed=True, group_keys=False, dropna=False)
        .agg({"stop_vp_distance_meters": "max"})
        .reset_index()
    )

    two_vp = pd.concat(
        [min_pos_distance, min_neg_distance], 
        axis=0, ignore_index=True
    )
    
    gdf2 = pd.merge(
        gdf,
        two_vp,
        on = group_cols + ["stop_vp_distance_meters"],
        how = "inner"
    )
    
    # since shape_meters actually might be decreasing as time progresses,
    # (bus moving back towards origin of shape)
    # we don't actually know that the smaller shape_meters is the first timestamp
    # nor the larger shape_meters is the second timestamp.
    # all we know is that stop_meters (stop) falls between these 2 shape_meters.
    # sort by timestamp, and set the order to be 0, 1    
    
    return gdf2


def consolidate_surrounding_vp(df, group_cols):
    df = df.assign(
        obs = (df.sort_values(group_cols + ["vp_idx"])
               .groupby(group_cols, 
                        observed=True, group_keys=False, dropna=False)
               .cumcount()
            )
    )
    
    if "stop_meters" not in group_cols:
        group_cols = group_cols + ["stop_meters"]
    
    group_cols2 = group_cols + ["stop_geometry"]
    prefix_cols = ["vp_idx", "shape_meters"]
    timestamp_cols = ["location_timestamp_local", "moving_timestamp_local"]
    
    
    vp_before_stop = df.loc[df.obs==0][group_cols2 + prefix_cols + timestamp_cols]
    vp_after_stop = df.loc[df.obs==1][group_cols2 + prefix_cols + timestamp_cols]
    
    # For the vp before the stop occurs, we want the maximum timestamp
    # of the last position
    # We want to keep the moving_timestamp (which is after it's dwelled)
    vp_before_stop = vp_before_stop.assign(
        prior_vp_timestamp_local = vp_before_stop.moving_timestamp_local,
    ).rename(
        columns = {**{i: f"prior_{i}" for i in prefix_cols}}
    ).drop(columns = timestamp_cols)
    
    # For the vp after the stop occurs, we want the minimum timestamp
    # of that next position
    # Keep location_timetamp (before it dwells)
    vp_after_stop = vp_after_stop.assign(
        subseq_vp_timestamp_local = vp_after_stop.location_timestamp_local,
    ).rename(
        columns = {**{i: f"subseq_{i}" for i in prefix_cols}}
    ).drop(columns = timestamp_cols)
    
    df_wide = pd.merge(
        vp_before_stop,
        vp_after_stop,
        on = group_cols2,
        how = "inner"
    )

    return df_wide


if __name__ == "__main__":
    
    start = datetime.datetime.now()
    
    gdf = merge_nearest_vp_with_shape(analysis_date)
    
    gdf2 = explode_vp_and_project_onto_shape(gdf, analysis_date)
    
    # These are defined in GTFS_DATA_DICT
    trip_stop_cols = [
        "trip_instance_key", "stop_sequence", 
    "stop_sequence1"] + ["shape_array_key", "stop_pair", "stop_meters"]
    
    gdf3 = find_two_closest_vp(gdf2, trip_stop_cols).sort_values(
        trip_stop_cols + ["vp_idx"]
    ).reset_index(drop=True)
    
    gdf4 = consolidate_surrounding_vp(gdf3, trip_stop_cols)
    
    utils.geoparquet_gcs_export(
        gdf4,
        SEGMENT_GCS,
        f"test_nearest2_vp_to_stop_{analysis_date}"
    )
    
    end = datetime.datetime.now()
    print(f"narrow down to 2 nearest vp (BBB): {end - start}")