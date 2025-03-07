import geopandas as gpd
import pandas as pd

from dask import delayed, compute

from shared_utils import rt_dates
from segment_speed_utils import segment_calcs
from update_vars import GTFS_DATA_DICT, SEGMENT_GCS

def merge_raw_and_grouped(
    vp: gpd.GeoDataFrame,
    vp_grouped: gpd.GeoDataFrame,
):
    """
    Merge vp and vp_grouped,
    convert timestamps to seconds, and check if the grouping is ok.
    """    
    merge_cols = [
        "gtfs_dataset_name",
        "trip_instance_key",
        "location_timestamp_local",
        "geometry"
    ]
    
    gdf = pd.merge(
        vp,
        vp_grouped,
        on = merge_cols,
        how = "outer",
        indicator=True
    ).sort_values("location_timestamp_local").reset_index(drop=True)
    
    group_cols = ["gtfs_dataset_name", "trip_instance_key"]
    
    gdf = gdf.assign(
        vp_idx = gdf.index,
        moving_timestamp_local = gdf.groupby(group_cols).moving_timestamp_local.ffill(),
        n_vp = gdf.groupby(group_cols).n_vp.ffill().astype("Int64"),        
    )
    
    gdf = segment_calcs.convert_timestamp_to_seconds(
        gdf, ["location_timestamp_local", "moving_timestamp_local"]
    ).drop(columns = ["location_timestamp_local", "moving_timestamp_local"])
    
    # https://stackoverflow.com/questions/71656436/pandas-groupby-cumcount-one-cumulative-count-rather-than-a-cumulative-count-fo/71657062
    gdf['vp_group'] = gdf.assign(
        temp = ~gdf.duplicated(
            subset=group_cols + ['moving_timestamp_local_sec']
        )
    ).groupby(group_cols)['temp'].cumsum()
    
    gdf2 = (gdf
        .groupby(group_cols + ["vp_group"])
        .agg(
            {"vp_idx": "nunique",
             "location_timestamp_local_sec": "min",
             "moving_timestamp_local_sec": "max",
             "n_vp": "mean"}
        ).reset_index()
       )
    
    return gdf2


if __name__ == "__main__":
    analysis_date = rt_dates.DATES["feb2025"]
    
    RAW_VP = GTFS_DATA_DICT.speeds_tables.raw_vp
    VP_GROUPED = GTFS_DATA_DICT.speeds_tables.raw_vp2

    subset_cols = [
        "gtfs_dataset_name",
        "trip_instance_key",
        "location_timestamp_local",
        "geometry"
    ]
    
    vp = delayed(gpd.read_parquet)(
        f"{SEGMENT_GCS}{RAW_VP}_{analysis_date}.parquet",
        columns = subset_cols
    ).dropna(subset="trip_instance_key")

    vp_grouped = delayed(gpd.read_parquet)(
        f"{SEGMENT_GCS}{VP_GROUPED}_{analysis_date}.parquet",
        columns = subset_cols + ["moving_timestamp_local", "n_vp"]
    ).dropna(subset="trip_instance_key")

    results = delayed(merge_raw_and_grouped)(vp, vp_grouped)

    results = compute(results)[0]
    results.to_parquet(f"{SEGMENT_GCS}comparison_{analysis_date}.parquet")