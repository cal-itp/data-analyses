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
Functions
'''
### Turn value counts into a dataframe ###
def value_function(df, column_of_int):
    df_new = (
    df[column_of_int].value_counts()
    .to_frame()
    .reset_index()
    .rename(columns = {column_of_int: 'values',
                       'index': column_of_int})
    )
    return df_new

### Adjust dollar amounts to millions ###
def millions(df, col_name: str): 
    df['Amt in M'] = (
    "$"
    + (df[col_name].astype(float) / 1000000)
    .round(0)
    .astype(str)
    + "M")
    return df 

### Style a dataframe ### 
def color(value):
    if value <  496292.00:
        color = "#E16B26"
    elif 496292.00 < value < 1435835:
        color =  "#EB9F3C"
    elif 1435836 <value < 3393431:
        color =  "#f6e7e1"
    elif 3393432 < value:
        color = "#8CBCCB"
    else:
        color =  "#2EA8CE"
    return f'background-color: {color}'


'''
Charts
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

### BASIC BAR CHART WITH INTERACTIVE TOOL TIP ### 
def basic_bar_chart(df, x_col, y_col, colorcol, chart_title=''):
    if chart_title == "":
        chart_title = (f"{labeling(x_col)} by {labeling(y_col)}")
    chart = (alt.Chart(df)
             .mark_bar()
             .encode(
                 x=alt.X(x_col, title=labeling(x_col), ),
                 y=alt.Y(y_col, title=labeling(y_col),sort=('-x')),
                 color = alt.Color(colorcol, 
                                  scale=alt.Scale(
                                      range=cp.CALITP_DIVERGING_COLORS),
                                      legend=alt.Legend(title=(labeling(colorcol)))
                                  ),
                tooltip = [x_col, y_col])
             .properties( 
                       title=chart_title)
    )

    chart=styleguide.preset_chart_config(chart)
    display(chart)
    return chart

### BASIC SCATTER CHART WITH INTERACTIVE TOOL TIP ### 
def basic_scatter(df, x_col, y_col, colorcol, chart_title=''):
    if chart_title == "":
        chart_title = (f"{labeling(x_col)} by {labeling(y_col)}")
    chart = (alt.Chart(df)
             .mark_circle(size = 100)
             .encode(
                 x=alt.X(x_col, title=labeling(x_col), ),
                 y=alt.Y(y_col, title=labeling(y_col),sort=('-x')),
                 color = alt.Color(colorcol, 
                                  scale=alt.Scale(
                                      range=cp.CALITP_DIVERGING_COLORS),
                                      legend=alt.Legend(title=(labeling(colorcol)))
                                  ),
                tooltip = [x_col, y_col])
             .properties( 
                       title=chart_title)
    )

    chart=styleguide.preset_chart_config(chart)
    display(chart)
    return chart
### BAR CHART WITH LABELS AND A LEGEND ###
#Base bar chart
def base_bar(df):
    chart = (alt.Chart(df)
             .mark_bar()
             )
    return chart

#Function
def fancy_bar_chart(df, LEGEND, y_col, x_col, label_col, chart_title=''):
    
    if chart_title == "":
        chart_title = (f"{labeling(x_col)} by {labeling(y_col)}")
    
    bar = base_bar(df)
    
    bar = (bar.encode(
         x=alt.X(x_col, title=labeling(x_col)),
         y=alt.Y(y_col, title=labeling(y_col), sort=('-x')),
         color=alt.Color(y_col, 
                        scale=alt.Scale(
                            domain=LEGEND, #Specifies the order of the legend.
                            range=cp.CALITP_DIVERGING_COLORS
                        )
                )
             )
            )
    #https://stackoverflow.com/questions/54015250/altair-setting-constant-label-color-for-bar-chart
    text = (bar
            .mark_text(align="left", baseline="middle",
                       color="black", dy=3
                      )
            .encode(text=label_col, 
                    # Set color here, because encoding for mark_text gets 
                    # superseded by alt.Color
                   color=alt.value("black"))
    )
      
    chart = (bar+text)
    
    chart = (styleguide.preset_chart_config(chart)
             .properties(title= chart_title).configure_axis(grid=False)
            )
    display(chart)
    return chart 