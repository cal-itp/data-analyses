"""
Add dwell time to vp
"""
import datetime
import numpy as np
import pandas as pd
import sys

from loguru import logger

from segment_speed_utils.project_vars import SEGMENT_GCS
from shared_utils import publish_utils
from update_vars import GTFS_DATA_DICT


def import_vp(analysis_date: str, **kwargs) -> pd.DataFrame:
    """
    Import vehicle positions for this script, 
    and allow for kwargs for filtering columns or rows from
    the partitioned parquet.
    """
    USABLE_VP = GTFS_DATA_DICT.speeds_tables.usable_vp

    df = pd.read_parquet(
        f"{SEGMENT_GCS}{USABLE_VP}_{analysis_date}/",
        **kwargs
    )
    
    return df


def filter_to_not_moving_vp(analysis_date: str) -> pd.DataFrame:
    """
    Filter down to vp that aren't moving, 
    because they have vp_primary_direction == Unknown.
    Exclude first vp of each trip.
    """
    first_vp = import_vp(
        analysis_date, 
        columns = ["trip_instance_key", "vp_idx"]
    ).groupby("trip_instance_key").vp_idx.min().tolist()
    
    # If the direction is Unknown, that means vp hasn't moved
    # from prior point
    # filter out the first vp for each trip (since that one row always has Unknown direction) 
    vp_staying = import_vp(
        analysis_date,
        columns = [
            "trip_instance_key", "vp_idx", 
            "location_timestamp_local", "vp_primary_direction"
        ],
        filters = [[("vp_primary_direction", "==", "Unknown"), 
                   ("vp_idx", "not in", first_vp)]]
    )
    
    return vp_staying


def group_vp_dwelling_rows(df: pd.DataFrame) -> pd.DataFrame: 
    """
    We do not know how many vp have repeated positions consecutively,
    but we want to consolidate as many as we can until it moves to 
    the next position.
    
    We know that the prior position has a vp index of vp_idx - 1.
    If it's not that, then it's moved on.
    This is important because buses can revisit a stop in a loop route,
    and it can stop at a plaza, go on elsewhere, and come back to a plaza,
    and we don't want to mistakenly group non-consecutive vp.
    """
    prior_expected = (df.vp_idx - 1).to_numpy()
    prior = (df.groupby("trip_instance_key").vp_idx.shift(1)).to_numpy()
    
    # Assign 0 if it seems to be dwelling (we want it to be grouped together)
    # 1 if it's moving
    same_group = np.where(prior == prior_expected, 0, 1)
    
    df = df.assign(
        same_group = same_group
    )
    
    return df


def assign_vp_groupings(
    unknown_vp: pd.DataFrame,
    analysis_date: str
):
    """
    Concatenate the unknown-direction vp with known-direction vp.
    A portion of unknown direction vps were flagged as being in the same
    group (consecutive timestamps and locations) or not.
    
    Use this to set a vp_grouping column that can help us do a more 
    nuanced grouping.
    Need to do this because it's possible for vp to show up in the same location
    but with timestamps far apart.
    """    
    subset_vp_idx = unknown_vp.vp_idx.tolist()
    
    known_vp = import_vp(
        analysis_date,
        columns = [
            "trip_instance_key", "vp_idx", 
            "location_timestamp_local", "vp_primary_direction"
        ],
        filters = [[("vp_idx", "not in", subset_vp_idx)]]
    )
    
    vp = pd.concat(
        [known_vp, unknown_vp], 
        axis=0
    ).sort_values(["trip_instance_key", "vp_idx"]).reset_index(drop=True)
    
    vp = vp.assign(
        same_group = vp.same_group.fillna(0).astype("int8")
    )
    
    
    vp = vp.assign(
        # since same_group=0 if the vp is dwelling,
        # cumsum() will not change from the prior vp
        # and a set of 2 or 3 will hold the same vp_grouping value
        # once the vp moves and is_moving=1, then cumsum() will increase again
        vp_grouping = (vp.groupby("trip_instance_key")
                   .same_group
                   .cumsum() 
                  )
    )
    
    return vp
    

def add_dwell_time(
    vp_grouped: pd.DataFrame,
) -> pd.DataFrame:
    """
    Take vp that have their groups flagged and 
    add dwell time (in seconds). Dwell time is calculated
    for this vp_location, which may not necessarily be a bus stop.
    """
    group_cols = ["trip_instance_key", "vp_grouping"]

    start_vp = (vp_grouped
                .groupby(group_cols, group_keys=False)
                .agg({
                    "vp_idx": "min",
                    "location_timestamp_local": "min",
                    "vp_primary_direction": "count"
                }).reset_index()
                .rename(columns = {"vp_primary_direction": "n_vp_at_location"})
               )

    end_vp = (vp_grouped
              .groupby(group_cols, group_keys=False)
              .agg({
                  "vp_idx": "max",
                  "location_timestamp_local": "max"
              }).reset_index()
               .rename(columns = {
                   "vp_idx": "end_vp_idx",
                   "location_timestamp_local": "moving_timestamp_local"
               })
    )
    
    df = pd.merge(
        start_vp,
        end_vp,
        on = group_cols,
        how = "inner"
    )
    
    df = df.assign(
        dwell_sec = (df.moving_timestamp_local - 
                     df.location_timestamp_local).dt.total_seconds().astype("int")
    )

    return df

if __name__ == "__main__":
    
    from update_vars import analysis_date_list

    LOG_FILE = "./logs/vp_preprocessing.log"
    logger.add(LOG_FILE, retention="3 months")
    logger.add(sys.stderr, 
               format="{time:YYYY-MM-DD at HH:mm:ss} | {level} | {message}", 
               level="INFO")
        
    for analysis_date in analysis_date_list:
        
        INPUT_FILE = GTFS_DATA_DICT.speeds_tables.usable_vp
        EXPORT_FILE = GTFS_DATA_DICT.speeds_tables.vp_dwell
        
        start = datetime.datetime.now()
        
        vp_unknowns = filter_to_not_moving_vp(analysis_date).pipe(group_vp_dwelling_rows) 
           
        vp_grouped = assign_vp_groupings(
            vp_unknowns, analysis_date
        )

        vp_with_dwell = add_dwell_time(vp_grouped)

        time1 = datetime.datetime.now()
        logger.info(f"compute dwell df: {time1 - start}")

        vp_usable = import_vp(analysis_date)

        vp_usable_with_dwell = pd.merge(
            vp_usable,
            vp_with_dwell,
            on = ["trip_instance_key", "vp_idx", "location_timestamp_local"],
            how = "inner"
        )

        publish_utils.if_exists_then_delete(
            f"{SEGMENT_GCS}{EXPORT_FILE}_{analysis_date}")

        vp_usable_with_dwell.to_parquet(
            f"{SEGMENT_GCS}{EXPORT_FILE}_{analysis_date}",
            partition_cols = "gtfs_dataset_key",
        )

        end = datetime.datetime.now()
        logger.info(f"merge with original and export: {end - time1}")
        logger.info(f"vp with dwell time {analysis_date}: {end - start}")
