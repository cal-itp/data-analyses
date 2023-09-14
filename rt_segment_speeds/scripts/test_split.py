"""
Transform df so that it is wide instead of long
prior to calculating speed.

For segments with 2 vp, we can do this.
For segments with 1 vp...set a placeholder for how to
fill in the previous coord?

Caveats to work into future function:
* pulling the prior vp can be from multiple segments ago
* we want to calculate distance between 2 points using shape and not segment
* the prior vp should just be vp_idx of current - 1
* check that it falls between the bounds of a trip's min_vp_idx and max_vp_idx
"""
import dask.dataframe as dd
import dask_geopandas as dg
import geopandas as gpd
import pandas as pd

from segment_speed_utils import helpers, segment_calcs, wrangle_shapes
from segment_speed_utils.project_vars import (SEGMENT_GCS, analysis_date, 
                                              CONFIG_PATH, PROJECT_CRS)
from shared_utils.geography_utils import WGS84

def get_prior_position_on_segment(
    df: pd.DataFrame,
    segment_identifier_cols: list,
    time_col: str,
) -> gpd.GeoDataFrame:
    """
    Get the prior vp on the segment.
    If a segment has 2 points, this will fill it in with a value.
    If it has 1 point, it returns NaN, so we will have to subset
    to those rows and fix those separately.
    """
    segment_trip_cols = ["trip_instance_key"] + segment_identifier_cols
    
    obs_per_segment_trip = (
        df.groupby(segment_trip_cols, 
                   observed=True, group_keys=False)
        .agg({"vp_idx": "count"})
        .reset_index()
        .rename(columns = {"vp_idx": "n_vp_seg"})
    )
    
    df2 = pd.merge(
        df,
        obs_per_segment_trip,
        on = segment_trip_cols,
        how = "inner"
    ).sort_values(
        segment_trip_cols + ["vp_idx"]
    ).reset_index(drop=True)
    
    df2 = df2.assign(
        prior_vp_idx = (df2.groupby(segment_trip_cols,
                                    observed=True, group_keys=False)
                        .vp_idx
                        .shift(1)
        )
    )
    
    df2 = df2.assign(
        prior_vp_idx = df2.prior_vp_idx.fillna(df2.vp_idx - 1).astype(int)
    )

    return df2
    

def get_usable_vp_bounds_by_trip(df: dd.DataFrame) -> pd.DataFrame:
    """
    Of all the usable vp, for each trip, find the min(vp_idx)
    and max(vp_idx).
    For the first stop, there will never be a previous vp to find,
    because the previous vp_idx will belong to a different operator/trip.
    But for segments in the middle of the shape, the previous vp can be anywhere,
    maybe several segments away.
    """
    
    grouped_df = df.groupby("trip_instance_key", 
                            observed=True, group_keys=False)

    start_vp = (grouped_df.vp_idx.min().reset_index()
                .rename(columns = {"vp_idx": "min_vp_idx"})
               )
    end_vp = (grouped_df.vp_idx.max().reset_index()
              .rename(columns = {"vp_idx": "max_vp_idx"})
             )
    
    df2 = dd.merge(
        start_vp,
        end_vp,
        on = "trip_instance_key",
        how = "left"
    ).reset_index(drop=True).compute()
    
    return df2
    
    
def put_all_together(
    analysis_date: str, 
    dict_inputs: dict = {}
):
    USABLE_VP = dict_inputs["stage1"]
    INPUT_FILE = dict_inputs["stage3"]
    SEGMENT_FILE = dict_inputs["segments_file"]
    SEGMENT_IDENTIFIER_COLS = dict_inputs["segment_identifier_cols"]
    GROUPING_COL = dict_inputs["grouping_col"]
    TIMESTAMP_COL = dict_inputs["timestamp_col"]

    # Import usable vp, which we'll use later for the x, y and timestamp
    usable_vp = dd.read_parquet(
        f"{SEGMENT_GCS}{USABLE_VP}_{analysis_date}",
        columns = ["trip_instance_key", "vp_idx", TIMESTAMP_COL, "x", "y"]
    )
    vp_idx_bounds = get_usable_vp_bounds_by_trip(usable_vp)
    
    # Start from pared down vp
    df = pd.read_parquet(
        f"{SEGMENT_GCS}vp_pare_down/{INPUT_FILE}_all_{analysis_date}",
        columns = SEGMENT_IDENTIFIER_COLS + ["trip_instance_key", "vp_idx"]
    )
    
    # Make sure all segments have 2 points
    # If it doesn't, fill it in with the previous vp_idx
    df2 = get_prior_position_on_segment(
        df, 
        SEGMENT_IDENTIFIER_COLS,
        TIMESTAMP_COL
    )
    
    # Check that the previous vp_idx actually occurs on the same trip
    df3 = pd.merge(
        df2,
        vp_idx_bounds,
        on = "trip_instance_key",
        how = "inner"
    )
    
    # For the first segment, if we only have 1 vp, we can't find a previous point
    # We'll use the next point then.
    # but make sure that we never use a point outside of that trip
    # later, we will have to use absolute value of difference in shape_meters
    # since distance must be positive
    df3 = df3.assign(
        prior_vp_idx = df3.apply(
            lambda x: 
            x.vp_idx + 1 if (x.prior_vp_idx < x.min_vp_idx) and 
            (x.vp_idx + 1 <= x.max_vp_idx)
            else x.prior_vp_idx, 
            axis=1)
    ).drop(columns = ["trip_instance_key", "min_vp_idx", "max_vp_idx"])
    
    # Merge in the timestamp and x, y coords 
    df_with_xy = dd.merge(
        usable_vp,
        df3,
        on = "vp_idx",
        how = "inner"
    )
    
    # Merge again to get timestamp and x, y coords of previous point
    usable_vp2 = usable_vp.rename(
        columns = {
            "vp_idx": "prior_vp_idx",
            TIMESTAMP_COL: f"prior_{TIMESTAMP_COL}",
            "x": "prior_x",
            "y": "prior_y",
        }
    ).drop(columns = "trip_instance_key")
    
    df_with_prior_xy = dd.merge(
        df_with_xy,
        usable_vp2,
        on = "prior_vp_idx",
        how = "inner"
    ).compute()
    
    gdf = gpd.GeoDataFrame(
        df_with_prior_xy,
        geometry = gpd.points_from_xy(df_with_prior_xy.x, df_with_prior_xy.y),
        crs = WGS84
    ).to_crs(PROJECT_CRS).drop(columns = ["x", "y"])
    
    gdf2 = gdf.assign(
        prior_geometry = gpd.points_from_xy(
            gdf.prior_x, gdf.prior_y, crs = WGS84
        ).to_crs(PROJECT_CRS)
    ).drop(columns = ["prior_x", "prior_y"]).set_geometry("geometry")
    
    return gdf2
    

    
    
if __name__ == "__main__":    
    


    gdf = gdf.assign(
        prior_time = gdf.prior_time.fillna(
            gdf.groupby("trip_instance_key", 
                        observed=True, group_keys=False)
            [time_col]
            .shift(1)
        ),
        prior_coord = gdf.geometry.fillna(
            gdf.groupby("trip_instance_key",
                        observed=True, group_keys=False)
            .geometry
            .shift(1)
        ),
    ).rename(columns = {"geometry": "vp_geometry"})

    
    segments = gpd.read_parquet(
        f"{SEGMENT_GCS}stop_segments_{analysis_date}.parquet",
        columns = SEGMENT_IDENTIFIER_COLS + ["geometry"]
    ).rename(columns = {"geometry": "segment_geometry"})
    
    two_obs_in_seg_gdf = pd.merge(
        two_obs_in_seg,
        segments,
        on = SEGMENT_IDENTIFIER_COLS,
        how = "inner"
    )
    
    two_obs_in_seg_gdf = dg.from_geopandas(two_obs_in_seg_gdf, npartitions=10
                                          ).set_geometry("vp_geometry")
    
    shapes_to_keep = one_obs_in_seg_one_outside.shape_array_key.unique().tolist()
    
    shapes = helpers.import_scheduled_shapes(
        analysis_date,
        columns = ["shape_array_key", "geometry"],
        filters = [[("shape_array_key", "in", shapes_to_keep)]],
        get_pandas = True,
        crs = "EPSG:3310"
    ).rename(columns = {"geometry": "shape_geometry"})
             
    one_obs_in_seg_one_outside_gdf = pd.merge(
        one_obs_in_seg_one_outside,
        shapes,
        on = "shape_array_key",
        how = "inner"
    )
    
    shape_meters_series = two_obs_in_seg_gdf.map_partitions(
        wrangle_shapes.project_point_geom_onto_linestring(
        "segment_geometry",
        "vp_geometry",
        meta = ("shape_meters", "float")
    ))
        
    two_obs_in_seg_gdf["shape_meters"] = shape_meters_series
    
    two_obs_in_seg_gdf = two_obs_in_seg_gdf.repartition(npartitions=5)
    two_obs_in_seg_gdf.to_parquet("two_seg_test")
    
    shape_meters_series2 =wrangle_shapes.project_point_geom_onto_linestring(
            one_obs_in_seg_one_outside_gdf,
            "shape_geometry",
            "vp_geometry",
        #meta = ("shape_meters", "float")
    )
    
    one_obs_in_seg_one_outside_gdf["shape_meters"] = shape_meters_series2
    one_obs_in_seg_one_outside_gdf.to_parquet("one_seg_test.parquet")
        
    