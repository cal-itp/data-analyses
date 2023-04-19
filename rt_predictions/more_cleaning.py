import pandas as pd

import assemble_stop_times
from download_stop_time_updates import snake_case_string
from segment_speed_utils.project_vars import analysis_date

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
    group_cols = assemble_stop_times.trip_cols
    
    stop_df = (df[group_cols + ["stop_sequence", "stop_id", 
                     "actual_stop_arrival_pacific"]]
               .drop_duplicates()
               .sort_values(group_cols + ["stop_sequence", 
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
    
    
if __name__ == "__main__":
    '''
    df = pd.DataFrame()

    for operator in OPERATORS:
        operator = snake_case_string(operator)

        subset_df = assemble_stop_times.import_stop_time_updates(
            analysis_date,
            operator = operator
        )
        df = pd.concat([df, subset_df], axis=0).reset_index(drop=True)

    df.to_parquet("combined_stop_time_updates.parquet")


    df = pd.DataFrame()

    for operator in OPERATORS:
        operator = snake_case_string(operator)
    
    subset_df = assemble_stop_times.import_final_trip_updates(
        analysis_date,
        operator = operator
    )
    df = pd.concat([df, subset_df], axis=0).reset_index(drop=True)
    
    df.to_parquet("combined_final_updates.parquet")
    '''
    
    st_updates = pd.read_parquet("combined_stop_time_updates.parquet")
    final_updates = pd.read_parquet("combined_final_updates.parquet")

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
    
    df3.to_parquet("rt_sched_stop_times.parquet")




