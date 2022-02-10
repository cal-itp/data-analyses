"""
5311 Utils for Analysis and Charting 
"""


import numpy as np
import pandas as pd
from siuba import *
from calitp import *
from plotnine import *
import intake
from shared_utils import geography_utils

import altair as alt
import altair_saver
from shared_utils import geography_utils
from shared_utils import altair_utils
from shared_utils import calitp_color_palette as cp
from shared_utils import styleguide




'''
charting functions 
'''
#Labels
def labeling(word):
    # Add specific use cases where it's not just first letter capitalized
    LABEL_DICT = { "prepared_y": "Year",
              "dist": "District",
              "nunique":"Number of Unique",
              "project_no": "Project Number"}
    
    if (word == "mpo") or (word == "rtpa"):
        word = word.upper()
    elif word in LABEL_DICT.keys():
        word = LABEL_DICT[word]
    else:
        #word = word.replace('n_', 'Number of ').title()
        word = word.replace('unique_', "Number of Unique ").title()
        word = word.replace('_', ' ').title()
    
    return word

# Bar
def basic_bar_chart(df, x_col, y_col):
    
    chart = (alt.Chart(df)
             .mark_bar()
             .encode(
                 x=alt.X(x_col, title=labeling(x_col), sort=('-y')),
                 y=alt.Y(y_col, title=labeling(y_col)),
                 #column = "payment:N",
                 color = alt.Color(y_col,
                                  scale=alt.Scale(
                                      range=altair_utils.CALITP_SEQUENTIAL_COLORS),
                                      legend=alt.Legend(title=(labeling(y_col)))
                                  ))
             .properties( 
                          title=(f"Highest {labeling(x_col)} by {labeling(y_col)}"))
    )

    chart=styleguide.preset_chart_config(chart)
    chart.save(f"./chart_outputs/bar_{x_col}_by_{y_col}.png")
    
    return chart


# Scatter 
def basic_scatter_chart(df, x_col, y_col, colorcol):
    
    chart = (alt.Chart(df)
             .mark_circle(size=60)
             .encode(
                 x=alt.X(x_col, title=labeling(x_col)),
                 y=alt.Y(y_col, title=labeling(y_col)),
                 #column = "payment:N",
                 color = alt.Color(colorcol,
                                  scale=alt.Scale(
                                      range=altair_utils.CALITP_SEQUENTIAL_COLORS),
                                      legend=alt.Legend(title=(labeling(colorcol)))
                                  ))
             .properties( 
                          title = (f"Highest {labeling(x_col)} by {labeling(y_col)}"))
    )

    chart=styleguide.preset_chart_config(chart)
    chart.save(f"./chart_outputs/scatter_{x_col}_by_{y_col}.png")
    
    return chart


# Line
def basic_line_chart(df, x_col, y_col):
    
    chart = (alt.Chart(df)
             .mark_line()
             .encode(
                 x=alt.X(x_col, title=labeling(x_col)),
                 y=alt.Y(y_col, title=labeling(y_col))
                                   )
              ).properties( 
                          title=f"{labeling(x_col)} by {labeling(y_col)}")

    chart=styleguide.preset_chart_config(chart)
    chart.save(f"./chart_outputs/line_{x_col}_by_{y_col}.png")
    
    return chart


# Bar chart without highest f'string
def basic_bar_chart2(df, x_col, y_col):
    
    chart = (alt.Chart(df)
             .mark_bar()
             .encode(
                 x=alt.X(x_col, title=labeling(x_col), sort=('-y')),
                 y=alt.Y(y_col, title=labeling(y_col)),
                 #column = "payment:N",
                 color = alt.Color(y_col,
                                  scale=alt.Scale(
                                      range=altair_utils.CALITP_SEQUENTIAL_COLORS),
                                      legend=alt.Legend(title=(labeling(y_col)))
                                  ))
             .properties( 
                          title=(f"{labeling(x_col)} by {labeling(y_col)}"))
    )

    chart=styleguide.preset_chart_config(chart)
    chart.save(f"./chart_outputs/bar_{x_col}_by_{y_col}.png")
    
    return chart


'''

Aggregating Functions

'''
#Aggregate by fleet size, GTFS, vehicle ages.
def aggregation_one(df, grouping_col):
    #adding up the vehicles 9+ and 15+ 
    df['vehicles_older_than_9']= df['_10_12'] + df['_13_15'] + df['_16_20'] + df['_21_25'] + df['_26_30'] + df['_31_60'] + df['_60plus']
    df['vehicles_older_than_15']= df['_16_20'] + df['_21_25'] + df['_26_30'] + df['_31_60'] + df['_60plus']
    #rename 0-9
    df = df.rename(columns={'_0_9':'vehicles_0_to_9'}) 
    #pivot 
    df = df.groupby([grouping_col]).agg({'vehicles_older_than_9':'sum', 'vehicles_older_than_15':'sum', 'vehicles_0_to_9': 'sum'}) 
    #dividing the different bins by the total across all agencies
    df['vehicles_percent_older_than_9'] = (df['vehicles_older_than_9']/sum(df['vehicles_older_than_9']))*100
    df['vehicles_percent_older_than_15'] = (df['vehicles_older_than_15']/sum(df['vehicles_older_than_15']))*100
    df['vehicles_percent_0_to_9'] = (df['vehicles_0_to_9']/sum(df['vehicles_0_to_9']))*100
    #reset index
    df = df.reset_index()
    return df 