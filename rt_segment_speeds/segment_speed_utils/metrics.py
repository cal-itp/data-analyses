"""
Define the metrics we can derive for 
segment speeds, RT vs schedule, etc.
"""
import pandas as pd

from typing import Literal

from segment_speed_utils import segment_calcs

def weighted_average_speeds_across_segments(
    df: pd.DataFrame,
    group_cols: list
) -> pd.DataFrame: 
    """
    We can use our segments and the deltas within a trip
    to calculate the trip-level average speed, or
    the route-direction-level average speed.
    But, we want a weighted average, using the raw deltas
    instead of mean(speed_mph), since segments can be varying lengths.
    """
    avg_speeds = (df.groupby(group_cols, 
                      observed=True, group_keys=False)
           .agg({
               "meters_elapsed": "sum",
               "sec_elapsed": "sum",
           }).reset_index()
          ).pipe(
        segment_calcs.speed_from_meters_elapsed_sec_elapsed
    )

    return avg_speeds
    

def derive_rt_vs_schedule_metrics(df: pd.DataFrame) -> pd.DataFrame:
    """
    Add metrics comparing RT vs schedule and do some numeric rounding.
    """
    integrify = ["vp_in_shape", "total_vp"]
    df[integrify] = df[integrify].fillna(0).astype("int")
    
    df = df.assign(
        vp_per_minute = df.total_vp / df.rt_service_minutes,
        pct_in_shape = df.vp_in_shape / df.total_vp,
        pct_rt_journey_atleast1_vp = df.minutes_atleast1_vp / df.rt_service_minutes,
        pct_rt_journey_atleast2_vp = df.minutes_atleast2_vp / df.rt_service_minutes,
        pct_sched_journey_atleast1_vp = (df.minutes_atleast1_vp / 
                                         df.scheduled_service_minutes),
        pct_sched_journey_atleast2_vp = (df.minutes_atleast2_vp / 
                                         df.scheduled_service_minutes),
    )
    
    two_decimal_cols = [
        "vp_per_minute", "rt_service_minutes", 
    ]
    
    df[two_decimal_cols] = df[two_decimal_cols].round(2)
    
    three_decimal_cols = [
        c for c in df.columns if "pct_" in c
    ]
    
    df[three_decimal_cols] = df[three_decimal_cols].round(3)

    # Mask percents for any values above 100%
    # Scheduled service minutes can be assumed to be shorter than 
    # RT service minutes, so there can be more minutes with vp data available
    # but vice versa can be true
    mask_me = [c for c in df.columns if 
               ("pct_sched_journey" in c) or 
               # check when this would happen in route direction aggregation
               ("pct_rt_journey" in c)]
    for c in mask_me:
        df[c] = df[c].mask(df[c] > 1, 1)    
    
    return df    


def derive_trip_comparison_metrics(
    df: pd.DataFrame,
    early_cutoff: int,
    late_cutoff: int
) -> pd.DataFrame: 
    """
    Add RT vs schedule trip comparison metrics that are
    more opinionated. Tweak these later based on operator feedback.
    We'll set the number of minutes for defining a
    trip as being early/ontime/late.
    """
    df = df.assign(  
        rt_sched_journey_difference = (df.rt_service_minutes - 
                                       df.scheduled_service_minutes)
    )

    df = df.assign(
        is_early = df.apply(
            lambda x: 
            1 if x.rt_sched_journey_difference < early_cutoff
            else 0, axis=1).astype(int),
        is_ontime = df.apply(
            lambda x: 
            1 if (x.rt_sched_journey_difference >= early_cutoff) and 
            (x.rt_sched_journey_difference <= late_cutoff)
            else 0, axis=1).astype(int), 
        is_late = df.apply(
            lambda x: 
            1 if x.rt_sched_journey_difference > late_cutoff
            else 0, axis=1).astype(int),
    )
    
    return df


def calculate_weighted_average_vp_schedule_metrics(
    df: pd.DataFrame, 
    group_cols: list,
) -> pd.DataFrame:
    
    sum_cols = [
        "minutes_atleast1_vp",
        "minutes_atleast2_vp",
        "rt_service_minutes",
        "scheduled_service_minutes",
        "total_vp",
        "vp_in_shape",
        "is_early", "is_ontime", "is_late"
    ]

    count_cols = ["trip_instance_key"]
    
    df2 = (
        df.groupby(group_cols,
                   observed=True, group_keys=False)
        .agg({
            **{e: "sum" for e in sum_cols}, 
            **{e: "count" for e in count_cols}}
        ).reset_index()
        .rename(columns = {"trip_instance_key": "n_vp_trips"})
    )
    
    # when we aggregate by any unit above trip,
    # summing up scheduled trip time and rt trip time is necessary,
    # but we should still get a new average
    
    
    
    return df2


def concatenate_peak_offpeak_allday_averages(
    df: pd.DataFrame, 
    group_cols: list,
    metric_type: Literal["segment_speeds", "rt_vs_schedule", "summary_speeds"]
) -> pd.DataFrame:
    """
    Calculate average speeds for all day and
    peak_offpeak.
    Concatenate these, so that speeds are always calculated
    for the same 3 time periods.
    """
    if metric_type == "segment_speeds":
        avg_peak = segment_calcs.calculate_avg_speeds(
            df,
            group_cols + ["peak_offpeak"]
        )

        avg_allday = segment_calcs.calculate_avg_speeds(
            df,
            group_cols
        ).assign(peak_offpeak = "all_day")
    
    elif metric_type == "summary_speeds":
        avg_peak = weighted_average_speeds_across_segments(
            df,
            group_cols + ["peak_offpeak"]
        )
        
        avg_allday = weighted_average_speeds_across_segments(
            df,
            group_cols
        ).assign(peak_offpeak = "all_day")   
    
    elif metric_type == "rt_vs_schedule":
        avg_peak = calculate_weighted_average_vp_schedule_metrics(
            df,
            group_cols + ["peak_offpeak"]
        )
        
        avg_allday = calculate_weighted_average_vp_schedule_metrics(
            df,
            group_cols
        ).assign(peak_offpeak = "all_day")
    
    else:
        print(f"Valid metric types: ['segment_speeds', 'summary_speeds', 'rt_vs_schedule']")
        
    # Concatenate so that every segment has 3 time periods: peak, offpeak, and all_day
    avg_metrics = pd.concat(
        [avg_peak, avg_allday], 
        axis=0, ignore_index = True
    ).rename(
        columns = {"peak_offpeak": "time_period"}
    )
        
    return avg_metrics