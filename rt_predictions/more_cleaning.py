"""
More data cleaning steps, specifically to 
reduce the columns we're bringing in for the metrics.
Instead of having arrival/departure time, since
usually only one is populated, we'll prefer 
the arrival one, and if that is missing, fill it in with departure.
"""
import pandas as pd

from dask import delayed, compute

import assemble_stop_times
from download_stop_time_updates import snake_case_string
from segment_speed_utils.project_vars import (PREDICTIONS_GCS, 
                                              analysis_date)

OPERATORS = [
    "Anaheim Resort",
    "Bay Area 511 Dumbarton Express",
    "Bay Area 511 Fairfield and Suisun Transit",
]


def resolve_missing_arrival_vs_departure(df: pd.DataFrame):
    """
    Sets of 2 columns for arrival/departure usually only populates one.
    Combine into 1 column. Start with arrival, and if it's missing, 
    fill it in with departure.
    """
    drop_cols = ["actual_stop_arrival_time_pacific", 
                 "actual_stop_departure_time_pacific", 
                 "arrival_time_pacific", 
                 "departure_time_pacific",
                ]

    # If actual stop arrival or departure time is populated,
    # pick arrival, and if NaT, fill in with departure
    # For predicted arrival_time, if it's missing, fill it in with predicted departure time
    df = df.assign(
        actual_stop_arrival_pacific = df.actual_stop_arrival_time_pacific.fillna(
            df.actual_stop_departure_time_pacific),
        predicted_pacific = df.arrival_time_pacific.fillna(
            df.departure_time_pacific)
    ).drop(columns = drop_cols)
    
    return df


def grab_prior_stop_actual_arrival(df: pd.DataFrame) -> pd.DataFrame:
    """
    Get the df to each individual stop, and grab the previous 
    stop arrival.
    We need this to subset down to prediction duration, which is
    the period between stops, for some of the metrics.
    """
    group_cols = assemble_stop_times.trip_cols
    
    stop_df = (df[group_cols + ["stop_sequence", "stop_id", 
                     "actual_stop_arrival_pacific"]]
               .drop_duplicates()
               .sort_values(group_cols + [
                   "stop_sequence", 
                   "actual_stop_arrival_pacific"])
               .reset_index(drop=True)
              )

    # Grab the previous stop's actual arrival time
    stop_df = stop_df.assign(
        prior_stop_arrival_pacific = (
            stop_df.sort_values(group_cols + ["stop_sequence"])
            .groupby(group_cols, group_keys=False
                    )["actual_stop_arrival_pacific"]
            .apply(lambda x: x.shift(1))
        )
    ).drop(columns = "actual_stop_arrival_pacific") 
    # we will merge this back onto the full df, so actual_stop_arrival_pacific will be there
    
    return stop_df


def concatenate_files(operator_list: list):
    """
    Since we downloaded individual operators for stop_time_updates
    and final_trip_updates, use dask.delayed 
    to read each in and concatenate into 1 parquet for export.
    """
    operator_snake_list = [snake_case_string(i) for i in operator_list]
    
    delayed_st_update_dfs = [
        delayed(assemble_stop_times.import_stop_time_updates)(
            analysis_date,
            operator = o) 
        for o in operator_snake_list
    ]
    
    delayed_final_update_dfs = [
        delayed(assemble_stop_times.import_final_trip_updates)(
            analysis_date,
            operator = o)
        for o in operator_snake_list
    ]
    
    def compute_delayed_dfs(
        list_of_delayed_pandas: list
    ) -> pd.DataFrame:
        """
        Compute delayed objects. Since these are pd.DataFrames, 
        we use pd.concat to return 1 df.
        dask_utils would need these to be dd.DataFrames to export correctly.
        """
        dfs = [compute(i)[0] for i in list_of_delayed_pandas]
        df = pd.concat(dfs, axis=0).reset_index(drop=True)

        return df
    
    st_df = compute_delayed_dfs(delayed_st_update_dfs)
    st_df.to_parquet(
        f"{PREDICTIONS_GCS}stop_time_updates_{analysis_date}.parquet")
    
    final_tu_df = compute_delayed_dfs(delayed_final_update_dfs)
    final_tu_df.to_parquet(
        f"{PREDICTIONS_GCS}final_updates_{analysis_date}.parquet"
    )
    
    
if __name__ == "__main__":
    
    concatenate_files(OPERATORS)
    
    st_updates = pd.read_parquet(
        f"{PREDICTIONS_GCS}stop_time_updates_{analysis_date}.parquet")
    final_updates = pd.read_parquet(
        f"{PREDICTIONS_GCS}final_updates_{analysis_date}.parquet")

    df = assemble_stop_times.get_usable_predictions(
        st_updates,
        final_updates,
        analysis_date, 
    )
    
    df2 = resolve_missing_arrival_vs_departure(df)

    stop_df = grab_prior_stop_actual_arrival(df2)
    
    df3 = pd.merge(
        df2, 
        stop_df, 
        on = assemble_stop_times.stop_cols + ["stop_sequence"],
        how = "inner",
    )
    
    df3.to_parquet(
        f"{PREDICTIONS_GCS}rt_sched_stop_times_{analysis_date}.parquet")
