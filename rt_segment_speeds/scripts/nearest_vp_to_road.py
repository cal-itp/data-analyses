"""
"""
import dask.dataframe as dd
import datetime
import geopandas as gpd
import numpy as np
import pandas as pd

from segment_speed_utils import helpers, wrangle_shapes
from segment_speed_utils.project_vars import (SEGMENT_GCS, 
                                              CONFIG_PATH, PROJECT_CRS)


road_id_cols = ["linearid", "mtfcc"]
segment_identifier_cols = road_id_cols + ["segment_sequence"]


def merge_vp_to_crosswalk(
    analysis_date: str, 
    filters: tuple
):
    # vp to road segment crosswalk
    df = pd.read_parquet(
        f"{SEGMENT_GCS}vp_sjoin/vp_road_segments_wide_{analysis_date}.parquet",
        filters = filters
    )
    
    # only keep the road segments that have at least 2 vp
    df = df.assign(
        n_vp = df.apply(lambda x: len(x.vp_idx_arr), axis=1)
    ).query('n_vp > 1').drop(columns = "n_vp").reset_index(drop=True)
    
    df_long = df.explode(
        "vp_idx_arr", 
        ignore_index=True
    ).rename(
        columns = {"vp_idx_arr": "vp_idx"}
    ).astype({"vp_idx": "int64"})
        
    # Turn series of arrays into 1d array
    #subset_vp = np.concatenate(np.asarray(df.vp_idx_arr))
    subset_vp = df_long.vp_idx.tolist()
    
    # Pull the vp info for ones that join to road segments
    vp_usable = dd.read_parquet(
        f"{SEGMENT_GCS}vp_usable_{analysis_date}",
        filters = [[("vp_idx", "in", subset_vp)]],
        columns = ["vp_idx", "x", "y"],
    )
    
    vp_with_roads = dd.merge(
        vp_usable,
        df_long,
        on = "vp_idx",
        how = "inner"
    )
    
    return vp_with_roads


def expand_relevant_road_segments(
    analysis_date: str,
    segment_identifier_cols: list = ["linearid", "mtfcc",
                                     "segment_sequence"],
    filtering = None
):
    sjoin_results = pd.read_parquet(
        f"{SEGMENT_GCS}vp_sjoin/vp_road_segments_{analysis_date}",
        columns = segment_identifier_cols
    ).drop_duplicates()
    
    
    full_road_info = gpd.read_parquet(
        f"{SEGMENT_GCS}segments_staging/"
        f"roads_with_cutpoints_long_{analysis_date}.parquet",
        filters = filtering
    )
    
    road_segments = gpd.read_parquet(
        f"{SEGMENT_GCS}road_segments_{analysis_date}",
        filters = [[("mtfcc", "in", ["S1100", "S1200"])]],
        columns = segment_identifier_cols + [
            "primary_direction", "destination"],
    ).merge(
        sjoin_results,
        on = segment_identifier_cols,
        how = "inner"
    ).merge(
        full_road_info,
        on = segment_identifier_cols,
        how = "inner"
    )
    
    return road_segments

if __name__ == "__main__":
    from segment_speed_utils.project_vars import analysis_date
    
    start = datetime.datetime.now()
    
    test_trips = [
        '00062c6db9dbef9c80f5ada74b31e257',
        '0009c78b48866a26d664ab00f67d1606',
        '00139041e36b607c7e10cb7ec023e837', 
        'ff780650b98209acf69a71a7dab2502c',
        'ff86898d6a8ff5df82912699133bf4b6',
        'ffb44943b394f891d2b2286bb3902305'
    ]
    
    vp = merge_vp_to_crosswalk(
        analysis_date,
        filters = [[("trip_instance_key", "in", test_trips)]]
    )
    
    vp = vp.repartition(npartitions=3)
    
    subset_roads = vp.linearid.unique().compute().tolist()
    
    road_segments = expand_relevant_road_segments(
        analysis_date,
        segment_identifier_cols = segment_identifier_cols,
        filtering = [[("linearid", "in", subset_roads)]],
    )
    
    road_dtypes = vp[road_id_cols].dtypes.to_dict()

    vp_projected = vp.map_partitions(
        wrangle_shapes.project_vp_onto_segment_geometry,
        road_segments,
        grouping_cols = road_id_cols,
        meta = {
            "vp_idx": "int64",
            **road_dtypes,
            "shape_meters": "float"},
        align_dataframes = False
    ).persist()
    
    # Merge vp with road segment info 
    # with projected shape meters against the full road 
    df_with_projection = dd.merge(
        vp,
        vp_projected,
        on = ["vp_idx"] + road_id_cols,
        how = "inner"
    ).drop(columns = ["x", "y"]).compute()
    
    df_with_projection.to_parquet(
        f"{SEGMENT_GCS}projection/vp_projected_roads_{analysis_date}.parquet"
    )
    
    df_with_projection_wide = (df_with_projection
                           .groupby(["trip_instance_key"] + road_id_cols)
                           .agg({
                               "vp_idx": lambda x: list(x),
                               "shape_meters": lambda x: list(x)})
                           .reset_index()
                           .rename(columns = {
                               "vp_idx": "vp_idx_arr",
                               "shape_meters": "shape_meters_arr"
                           })
                  )
    
    # Now merge road segments with each destination acting as the road's stop
    # and merge on arrays of projected vp against that road
    gdf = pd.merge(
        road_segments,
        df_with_projection_wide,
        on = road_id_cols,
        how = "inner"
    )
    
    nearest_vp_idx = []
    subseq_vp_idx = []

    for row in gdf.itertuples():

        this_stop_meters = getattr(row, "road_meters")
        valid_shape_meters_array = getattr(row, "shape_meters_arr")
        valid_vp_idx_array = np.asarray(getattr(row, "vp_idx_arr"))

        idx = np.searchsorted(
            valid_shape_meters_array,
            this_stop_meters,
            side="right" 
            # want our stop_meters value to be < vp_shape_meters,
            # side = "left" would be stop_meters <= vp_shape_meters
        )

        # For the next value, if there's nothing to index into, 
        # just set it to the same position
        # if we set subseq_value = getattr(row, )[idx], 
        # we might not get a consecutive vp
        nearest_value = valid_vp_idx_array[idx-1]
        subseq_value = nearest_value + 1

        nearest_vp_idx.append(nearest_value)
        subseq_vp_idx.append(subseq_value)
        
    result = gdf[segment_identifier_cols + [
        "primary_direction", "fullname", "road_meters", 
        "trip_instance_key"]]

    # Now assign the nearest vp for each trip that's nearest to
    # a given stop
    # Need to find the one after the stop later
    result = result.assign(
        nearest_vp_idx = nearest_vp_idx,
        subseq_vp_idx = subseq_vp_idx,
    )
    
    result.to_parquet(
        f"{SEGMENT_GCS}nearest_vp_roads_{analysis_date}.parquet"
    )
    
    end = datetime.datetime.now()
    print(f"execution time: {end - start}")