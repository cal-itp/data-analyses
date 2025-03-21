
"""
Quarterly Rollup Functions
"""
def quarterly_rollup(df:pd.DataFrame, metric_columns:list)->pd.DataFrame:
    """
    roll up months to each quarter for certain metrics.
    """
    quarterly_metrics = segment_calcs.calculate_weighted_averages(
    
    df=df,
    group_cols=[
        "quarter",
        "Period",
        "Organization",
        "Route",
        "dir_0_1",
        "Direction",
    ],
    metric_cols= metric_columns,
    weight_col="# Trips with VP",
    )
    return quarterly_metrics

def rollup_schd_qtr(peak_offpeak_df:pd.DataFrame)->pd.DataFrame:
    """
    Roll up # Scheduled Trips to be on a quarterly basis
    since this metric doesn't change very often. 
    """
    # Aggregate
    agg1 = (
    peak_offpeak_df.groupby(
        ["quarter", "Period", "Organization", "Route", "dir_0_1", "Direction"]
    )
    .agg({"Date":"nunique","# scheduled trips": "sum"})
    .reset_index()
    )
    
    # If a quarter is complete with all 3 months, divide by 3
    agg1.loc[agg1["Date"] == 3, "# scheduled trips"] = (
    agg1.loc[agg1["Date"] == 3, "# scheduled trips"] / 3)
    
    # If a quarter is incomplete with only 2 months, divide by 2 
    agg1.loc[agg1["Date"] == 2, "# scheduled trips"] = (
    agg1.loc[agg1["Date"] == 2, "# scheduled trips"] / 2
)
    return agg1

    # Roll up monthly metrics to quarterly
    # Filter for only rows that are "all day" statistics
    all_day = df.loc[df["Period"] == "all_day"].reset_index(drop=True)
    
    # Filter for only rows that are "peak/offpeak" statistics
    peak_offpeak_df = df.loc[df["Period"] != "all_day"].reset_index(drop=True)
    
    # Roll up some metrics that don't change too much
    # to be quarterly instead of monthly
    quarter_rollup_all_day = quarterly_rollup(all_day, [
        "Average VP per Minute",
        "% VP within Scheduled Shape",
        "Average Scheduled Service (trip minutes)",
        "ruler_100_pct",
        "ruler_for_vp_per_min"
    ]) 
    
    total_scheduled_trips = rollup_schd_qtr(peak_offpeak_df)
    
    # Merge these 2 
    m1 = pd.merge(quarter_rollup_all_day, total_scheduled_trips)
    print("Saved Digest RT")
    