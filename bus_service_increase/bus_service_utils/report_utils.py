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

integer_cols = ["E", "F"]
one_decimal_cols = ["A", "D", ]
two_decimal_cols = ["C"]

(df.astype(
    {c: "Int64" for c in integer_cols})
 .style.format(subset=one_decimal_cols, 
     formatter = {c: '{:,.1f}' for c in one_decimal_cols})
 .format(subset=two_decimal_cols,
         formatter = {c: '{:,.2f}' for c in two_decimal_cols}
 )
)

# Currency: https://stackoverflow.com/questions/35019156/pandas-format-column-as-currency

'''


def style_table(
    df: pd.DataFrame, 
    rename_cols: dict = {}, 
    drop_cols: list = [], 
    integer_cols: list = [],
    one_decimal_cols: list = [],
    two_decimal_cols: list = [],
    three_decimal_cols: list = [],
    currency_cols: list = [],
    percent_cols: list = [],
    left_align_cols: list = "first", # by default, left align first col
    center_align_cols: list = "all", # by default, center align all other cols
    right_align_cols: list = [],
    custom_format_cols: dict = {},
    display_table: bool = True
) -> pd.io.formats.style.Styler | str: 
    """
    Returns a pandas Styler object with some basic formatting.
    Any other tweaks for currency, percentages, etc should be done before / after.
    """
    df = (df.drop(columns = drop_cols)
           .rename(columns = rename_cols)
          )
    
    if len(integer_cols) > 0:
        df = df.astype({c: "Int64" for c in integer_cols})
        
    if left_align_cols == "first":
        left_align_cols = list(df.columns)[0]
    if center_align_cols == "all":
        center_align_cols = list(df.columns)
        # all other columns except first one is center aligned
        center_align_cols = [c for c in center_align_cols if c not in left_align_cols]
    
    df_style = (df.style
                .format(#subset = integer_cols, 
                        formatter = {c: '{:,g}' for c in integer_cols})
                .format(#subset = one_decimal_cols, 
                        formatter = {c: '{:,.1f}' for c in one_decimal_cols})
                .format(#subset = two_decimal_cols,
                       formatter = {c: '{:,.2f}' for c in two_decimal_cols})
                .format(#subset = three_decimal_cols,
                        formatter = {c: '{:,.3f}' for c in three_decimal_cols})
                .format(#subset = percent_cols,
                         formatter = {c: '{:,.2%}' for c in percent_cols})
                .format(#subset = currency_cols,
                        formatter = {c: '$ {:,.2f}' for c in currency_cols})
                .set_properties(subset=left_align_cols, 
                                **{'text-align': 'left'})
                .set_properties(subset=center_align_cols, 
                               **{'text-align': 'center'})
                .set_properties(subset=right_align_cols, 
                                **{'text-align': 'right'})
                .set_table_styles([dict(selector='th', 
                                        props=[('text-align', 'center')])
                                        ])
           .hide(axis="index")
          )
    
    
    def add_custom_format(
        df_style, #: styler object , 
        format_str: str, cols_to_format: list,): #-> #styler object: 
        """
        Appends any additional formatting needs.
            key: format string, such as '{:.1%}'
            value: list of columns to apply that formatter to.
        """
        new_styler = (df_style
                      .format(subset = cols_to_format,
                              formatter = {c: format_str for c in cols_to_format})
                     )

        return new_styler
    
    
    if len(list(custom_format_cols.keys())) > 0:
        for format_str, cols_to_format in custom_format_cols.items():
            df_style = add_custom_format(df_style, format_str, cols_to_format)

    if display_table is True: 
        display(HTML(df_style.to_html()))
    
    return df_style
        

# Display a table of route-level stats for each route_group
# Displaying route_name makes chart too crowded    
def style_route_stats(df):
    df = df.assign(
        route_short_name = df.apply(lambda x: 
                                    x.route_long_name if x.route_short_name is None
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