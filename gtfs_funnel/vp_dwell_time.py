"""
Add dwell time to vp
"""
import datetime
import pandas as pd
import sys

from dask import delayed, compute
from loguru import logger

from segment_speed_utils import helpers, segment_calcs
from segment_speed_utils.project_vars import SEGMENT_GCS
from update_vars import GTFS_DATA_DICT

def import_vp(analysis_date: str) -> pd.DataFrame:
    """
    Import vehicle positions with a subset of columns
    we need to check whether bus is dwelling
    at a location.
    """
    USABLE_VP = GTFS_DATA_DICT.speeds_tables.usable_vp
    vp = pd.read_parquet(
        f"{SEGMENT_GCS}{USABLE_VP}_{analysis_date}",
        columns = [
            "trip_instance_key", "vp_idx", 
            "location_timestamp_local", "vp_primary_direction"
        ],
    )
        
    return vp


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
    df = df.assign(
        #prior_expected = df.vp_idx - 1,
        prior = (df.sort_values(["trip_instance_key", "vp_idx"])
                .groupby("trip_instance_key", observed=True, group_keys=False)
                .vp_idx
                .apply(lambda x: x.shift(1))
               )
    )


    df = df.assign(
        # flag whether it is moving (we want 0's to show up for dwelling vp 
        # because this will get the cumcount() to work 
        is_moving = df.apply(
            lambda x: 
            0 if x.prior == x.prior_expected
            else 1, axis=1).astype("int8")
    )
    
    return df


def split_into_moving_and_dwelling(vp: pd.DataFrame):
    """
    Use vp_primary_direction to split vp into either moving vp or dwelling vp.
    Dwelling vp need extra transforms to figure how long it dwelled.
    It's unknown if there was no movement, because the x, y is the 
    same, so direction was not able to be calculated.
    The only exception is the first vp, because there is no prior point against which
    to calculate direction.
    """
    usable_bounds = segment_calcs.get_usable_vp_bounds_by_trip(
        vp
    ).drop(columns = "max_vp_idx")

    vp2 = pd.merge(
        vp, 
        usable_bounds,
        on = "trip_instance_key",
        how = "inner"
    )
    
    vp2 = vp2.assign(
        prior_expected = vp2.vp_idx - 1,
    )
    
    # keep subset of prior vp when we have unknowns, 
    #then we want to grab just the one above
    subset_vp_prior = vp2[
        vp2.vp_primary_direction=="Unknown"
    ].prior_expected.unique().tolist()
    
    subset_unknown_vp = vp2[
        vp2.vp_primary_direction=="Unknown"
    ].vp_idx.unique().tolist()
    
    # These vp have unknowns and may need to consolidate 
    # leave first vp in, just in case the second vp is unknown
    vp_unknowns = vp2.loc[
        vp2.vp_idx.isin(subset_vp_prior + subset_unknown_vp)
    ]

    # Vast majority of vp should be here, and we want to 
    # separate these out because no change is happening
    # and we don't want to do an expensive row-wise shift on these
    vp_knowns = vp2.loc[~vp2.vp_idx.isin(subset_vp_prior + subset_unknown_vp)]
    
    vp_unknowns2 = group_vp_dwelling_rows(vp_unknowns)
    
    vp3 = pd.concat(
        [vp_knowns, vp_unknowns2], 
        axis=0, ignore_index=True
    ).drop(
        columns = ["prior", "prior_expected"]
    ).fillna(
        {"is_moving": 1}
    ).astype(
        {"is_moving": "int8"}
    ).sort_values("vp_idx").reset_index(drop=True)
    
    vp3 = vp3.assign(
        # since is_moving=0 if the vp is dwelling,
        # cumsum() will not change from the prior vp
        # and a set of 2 or 3 will hold the same vp_grouping value
        # once the vp moves and is_moving=1, then cumsum() will increase again
        vp_grouping = (vp3.groupby("trip_instance_key", 
                                  observed=True, group_keys=False)
                   .is_moving
                   .cumsum() 
                  )
    )
    
    return vp3


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
                .groupby(group_cols, observed=True, group_keys=False)
                .agg({
                    "vp_idx": "min",
                    "location_timestamp_local": "min",
                    "vp_primary_direction": "count"
                }).reset_index()
                .rename(columns = {"vp_primary_direction": "n_vp_at_location"})
               )

    end_vp = (vp_grouped
              .groupby(group_cols, observed=True, group_keys=False)
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
    
        vp = delayed(import_vp)(analysis_date)

        vp_grouped = delayed(split_into_moving_and_dwelling)(vp)

        vp_with_dwell = delayed(add_dwell_time)(vp_grouped)

        vp_with_dwell = compute(vp_with_dwell)[0]

        time1 = datetime.datetime.now()
        logger.info(f"compute dwell df: {time1 - start}")

        vp_usable = pd.read_parquet(
            f"{SEGMENT_GCS}{INPUT_FILE}_{analysis_date}",
        )

        vp_usable_with_dwell = pd.merge(
            vp_usable,
            vp_with_dwell,
            on = ["trip_instance_key", "vp_idx", "location_timestamp_local"],
            how = "inner"
        )

        helpers.if_exists_then_delete(
            f"{SEGMENT_GCS}{EXPORT_FILE}_{analysis_date}")

        vp_usable_with_dwell.to_parquet(
            f"{SEGMENT_GCS}{EXPORT_FILE}_{analysis_date}",
            partition_cols = "gtfs_dataset_key",
        )

        end = datetime.datetime.now()
        logger.info(f"merge with original and export: {end - time1}")
        logger.info(f"vp with dwell time {analysis_date}: {end - start}")