import dask.dataframe as dd
import dask.dataframe as dd
import pandas as pd


# https://stackoverflow.com/questions/58145700/using-groupby-to-store-value-counts-in-new-column-in-dask-dataframe
# https://github.com/dask/dask/pull/5327
def keep_min_max_timestamps_by_segment(
    vp_to_seg: dd.DataFrame, 
    segment_cols: list,
    timestamp_col: str = "location_timestamp"
) -> dd.DataFrame:
    """
    For each segment-trip combination, throw away excess points, just 
    keep the enter/exit points for the segment.
    """
    segment_trip_cols = ["gtfs_dataset_key", "trip_id"] + segment_cols
    
    # https://stackoverflow.com/questions/52552066/dask-compute-gives-attributeerror-series-object-has-no-attribute-encode    
    # comment out .compute() and just .reset_index()
    enter = (vp_to_seg.groupby(segment_trip_cols)
             [timestamp_col].min()
             #.compute()
             .reset_index()
            )

    exit = (vp_to_seg.groupby(segment_trip_cols)
            [timestamp_col].max()
            #.compute()
            .reset_index()
           )
    
    enter_exit = dd.multi.concat([enter, exit], axis=0)
    
    # Merge back in with original to only keep the min/max timestamps
    # dask can't sort by multiple columns to drop
    enter_exit_full_info = dd.merge(
        vp_to_seg,
        enter_exit,
        on = segment_trip_cols + [timestamp_col],
        how = "inner"
    ).reset_index(drop=True)
        
    return enter_exit_full_info