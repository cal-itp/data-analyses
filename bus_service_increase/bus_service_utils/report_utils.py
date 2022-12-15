"""
Common functions for styling notebooks 
published as reports.
"""
import pandas as pd
import pandas.io.formats.style # to add type hint (https://github.com/pandas-dev/pandas/issues/24884)

from IPython.display import HTML

'''
Working example:
https://github.com/CityOfLosAngeles/planning-entitlements/blob/master/notebooks/D1-entitlement-demographics.ipynb

# Currency: https://stackoverflow.com/questions/35019156/pandas-format-column-as-currency
'''
        
# Display a table of route-level stats for each route_group
# Displaying route_name makes chart too crowded    
def style_route_stats(df):
    df = df.assign(
        route_short_name = df.apply(
            lambda x: x.route_long_name if x.route_short_name is None
            else x.route_short_name, axis=1)
    )
    
    # Rename columns for display
    rename_cols = {
        "route_id2": "Route ID",
        "route_short_name": "Route Name",
        "route_group": "Route Group",
        "num_trips": "# trips",
        "daily_avg_freq": "Daily Avg Freq (trips per hr)",
        "pm_peak_freq": "PM Peak Avg Freq (trips per hr)",
        "percentiles": "25th, 50th, 75th ptile (hrs)",
        "mean_speed_mph": "Avg Daily Speed (mph)",
    }
    
    # Style it
    drop_cols = [
        "calitp_itp_id", "route_id", "route_group", 
        "pct_trips_competitive",
        "p25", "p50", "p75",
        "category"
    ]
    
    # Change alignment for some columns
    # https://stackoverflow.com/questions/59453091/left-align-the-first-column-and-center-align-the-other-columns-in-a-pandas-table
    df_style = (df.sort_values(
        ["pct_trips_competitive", "route_id2"], 
        ascending=[False , True])
           .drop(columns = drop_cols)
           .rename(columns = rename_cols)
           .style.format(
               subset=['Daily Avg Freq (trips per hr)', 
                       'PM Peak Avg Freq (trips per hr)', 
                       'Avg Daily Speed (mph)', 
                      ], 
               **{'formatter': '{:,.3}'})
                .set_properties(subset=['Route ID', 'Route Name'], 
                                **{'text-align': 'left'})
                .set_properties(subset=['# trips', 'Daily Avg Freq (trips per hr)', 
                                       'PM Peak Avg Freq (trips per hr)', 
                                        'Avg Daily Speed (mph)',
                                       ], 
                               **{'text-align': 'center'})
                .set_table_styles([dict(selector='th', 
                                        props=[('text-align', 'center')])
                                        ])
           .hide(axis="index")
           .to_html()
          )
    
    display(HTML("<h4>Route Stats</h4>"))
    display(HTML(df_style))