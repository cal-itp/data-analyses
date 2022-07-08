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

GCS_FILE_PATH = "gs://calitp-analytics-data/data-analyses/transit_stacks/"

'''
Basic Cleaning
Useful for the few different 
Transitstacks datasets in Airtable
'''
def basic_cleaning(path_file:str, col_split:str, columns_to_count:list):
    #Read in
    df = to_snakecase(pd.read_csv(f"{GCS_FILE_PATH}{path_file}"))
    
    #Fill in nulls & drop duplicates
    df = df.fillna('N/A').drop_duplicates()
    
    #For all columns, take out quotations
    df = df.replace('"', '', regex=True)
    '''
    De duplicate: in Airtable, certain elements that are the same are repeated several times
    which is not necessary in our analysis. Also remove unnecessary quotation marks
    https://stackoverflow.com/questions/56466917/is-there-a-way-in-pandas-to-remove-duplicates-from-within-a-series
    '''
    df[col_split] = (
    df[col_split]
    .apply(lambda x: ", ".join(set([y.strip() for y in x.split(",")])))
    .str.strip())
    
    '''
    Count number of elements delinated by commas in a column
    '''
    for c in columns_to_count:
        # Create a new column for counted elements
        df[f"number_of_{c}"] =  (df[c]
        .apply(lambda x: len(x.split(","))) 
        .astype("int64")
        ) 
        
    return df 

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

# Bar Chart
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
                                      range=altair_utils.CALITP_CATEGORY_BRIGHT_COLORS),
                                      legend=alt.Legend(title=(labeling(colorcol)))
                                  ))
             .properties( 
                       title=chart_title)
    )

    chart=styleguide.preset_chart_config(chart)
    chart.save(f"./bar_{chart_title}.png")
    return chart

### Bar chart with labels  ###
#Base bar chart
def base_bar(df):
    chart = (alt.Chart(df)
             .mark_bar()
             )
    return chart

# Fancier Bar Chart
def fancy_bar_chart(df, x_col, y_col, label_col, chart_title=''):
    
    if chart_title == "":
        chart_title = (f"{labeling(x_col)} by {labeling(y_col)}")
    
    bar = base_bar(df)
    
    bar = (bar.encode(
         x=alt.X(x_col, title=labeling(x_col)),
         y=alt.Y(y_col, title=labeling(y_col), sort=('-x')),
         color=alt.Color(y_col, 
                        scale=alt.Scale(
                            range=altair_utils.CALITP_CATEGORY_BRIGHT_COLORS
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
    chart.save(f"./bar_{chart_title}.png")
    display(chart)
    
'''
Other functions
'''
#Count elements in a column, delinated by commas: without any grouping
def count_by_commas(df, col_to_count: str, new_col_name:str): 
    df[new_col_name] = (
    df[col_to_count]
    .apply(lambda x: len(x.split(",")))
    .astype("int64")) 
    return df 

#Grab value counts and turn it into a dataframe
def value_counts_df(df, col_of_interest):
    df = (
    df[col_of_interest].value_counts()
    .to_frame()
    .reset_index()
    )
    return df 
    
### Count number of strings within a column, separated out by columns and group them by "name" column.
# https://stackoverflow.com/questions/51502263/pandas-dataframe-object-has-no-attribute-str
def number_of_elements(df, column_use, column_new):
    df[column_new] = (
        df[column_use]
        .str.split(",+")
        .str.len()
        .groupby(df.services)
        .transform("sum")
    )
    return df 

#Puts all the elements in the column "col to summarize" 
#onto one line and separates it by commas. 
#For ex: an agency typically purchases 1+ products,and each product has its own line: 
#this function grabs all the products by agency and puts it on the same line.
def summarize_rows(df, col_to_group: str, col_to_summarize: str):
    df_col_to_summarize = (df.groupby(col_to_group)[col_to_summarize]
    .apply(','.join)
    .reset_index())
    return df_col_to_summarize

#Groups dataframe, aggregates it by unique count, sorts by the aggregated col
#Returns top 10 values from largest to smallest
def summarize_top_ten(df, group_by_col: str, agg_and_sort_col: str): 
    df = (
    df.groupby(group_by_col)
    .agg({agg_and_sort_col: "nunique"})
    .sort_values(agg_and_sort_col, ascending=False)
    .reset_index()
    .head(10)
    )
    return df 