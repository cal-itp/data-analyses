"""
Analysis Functions

"""

import numpy as np
import pandas as pd
from calitp import to_snakecase
from siuba import *
import intake
import geopandas as gpd
from plotnine import *

import altair as alt
import altair_saver

from shared_utils import geography_utils
from shared_utils import altair_utils
from shared_utils import calitp_color_palette as cp
from shared_utils import styleguide

import ipywidgets as widgets
from ipywidgets import *
from IPython.display import Markdown
from IPython.core.display import display

## add to notebook cell
# alt.themes.register("calitp_theme", styleguide.calitp_theme)
# alt.themes.enable("calitp_theme")


# aggfunc for specified columns

def calculate_data_all(df, col, aggregate_by=["dist"], aggfunc="sum"):
    df = (df.groupby(aggregate_by)
          .agg({col: aggfunc})
          .reset_index()
          .sort_values(col, ascending=False)
         )
    #df = df[df[col] > 0]
    df = (df>>filter(_[col] >0))
    return df


# get the top 20

def calculate_data_head(df, col, aggregate_by=["dist"], aggfunc="sum"):
    df1 = calculate_data_all(df, col, aggregate_by, aggfunc)
    #df = df[df[col] > 0]
    df1 = (df1>>arrange(-_[col])).head(20)
    return df1


#from Tiffany's branch DLA functions
#get basic information from the different columns by year

def count_all_years(df):
    count_years = geography_utils.aggregate_by_geography(
        df, 
        group_cols = ["prepared_y", "dist"],
        sum_cols = ["total_requested", "ac_requested", "fed_requested"],
        mean_cols = ["total_requested", "ac_requested", "fed_requested"],
        nunique_cols = ["primary_agency_name", "prefix", "project_no", "project_location", "type_of_work"]
    ).sort_values(["prepared_y", "dist"], ascending=[False, True])

    return count_years


#reading in catalog and merging with df

# def read_catalog(df):    
#     catalog = intake.open_catalog("catalog.yml")

#     city_boundary = catalog.ca_open_data.city_boundary.read()
#     county_bound = catalog.ca_open_data.county_boundary.read()
 
#     district_bound= catalog.district_bound.read()
#     rtpa_bound= catalog.rtpa_bound.read()
#     locode_df = pd.concat(pd.read_excel('gs://calitp-analytics-data/data-analyses/dla/e-76Obligated/locodes_updated7122021.xlsx', sheet_name=None), ignore_index=True)
#     locode_df = to_snakecase(locode_df)

#     #renaming
#     county_bound['name'] =  county_bound['name'] + ' County'
#     county_bound.rename(columns={'name': 'county_name', 'geometry': 'geometry2'}, inplace=True)

#     # deleting Calaveras County because the location of the project is not in district 7
#     delete_row = df[df["primary_agency_name"]== 'Calaveras County'].index
#     df = df.drop(delete_row)

#     new_df1 = pd.merge(df, locode_df,  how='left', left_on=['primary_agency_name'], right_on = ['agency_name'])
#     new_df2 = pd.merge(new_df1, city_bound,  how='left', left_on=['primary_agency_name'], right_on = ['NAME'])
#     new_df3 = left_join(new_df2, county_bound, on = "county_name")

#     return new_df3





#Function to find unqiue prefix and chart
#config plotnine chart to styleguide from Tiffany's shared_utils

def preset_plotnine_config(chart):
    chart = (chart
             + theme_538()
             + theme(plot_background=element_rect(fill=backgroundColor, color=backgroundColor),
                     panel_background=element_rect(fill=backgroundColor, color=backgroundColor),
                     panel_grid_major_y=element_line(
                        color=axisColor, linetype='solid', size=1),
                     panel_grid_major_x=element_blank(),
                     figure_size=(7.0, 4.4),
                     title=element_text(weight="bold", size=font_size, 
                                        family=font, color=blackTitle),
                     axis_title=element_text(family=labelFont, size=12, color=guideTitleColor),
                     axis_text=element_text(family=labelFont, size=10, color=guideLabelColor, 
                                            margin={'r': 4}
                                           ),
                     axis_title_x=element_text(margin={'t': 10}),
                     axis_title_y=element_text(margin={'r': 10}),
                     legend_title=element_text(font=labelFont, size=14, color=blackTitle, 
                                               margin={'b': 10}),
                     legend_text=element_text(font=labelFont, size=11, color=blackTitle, 
                                              margin={'t': 5, 'b': 5, 'r': 5, 'l': 5}),
                    )
    )
    
    return chart


"""
Charting
"""



def prefix_all_agencies(df, prefix_unique):
    
    # graphs 
    prefixes = df[df.prefix== prefix_unique]
    
    prefix_count_num = (prefixes >> count(_.primary_agency_name) >> arrange(-_.n)).head(50)
    
    prefix_count = (prefixes >> count(_.primary_agency_name) >> arrange(-_.n)).head(20)
    
    display(Markdown(f"**The number of agencies using {prefix_unique} is {len(prefix_count_num)}**"))
    
    # for the table- using one as some agencies only have one entry
    display(df[(df.prefix == prefix_unique)].sample(1))
    
    
    ax1 = (prefix_count
            >> ggplot(aes("primary_agency_name", "n", fill="primary_agency_name")) 
                + geom_col() 
                + theme(axis_text_x = element_text(angle = 45 , hjust=1))
                + labs(title='Top Agencies using Prefix', x='Agency', y='Number of Obligations', fill="Agency")
                + theme_538()
                + theme(plot_background=element_rect(fill=backgroundColor, color=backgroundColor),
                     panel_background=element_rect(fill=backgroundColor, color=backgroundColor),
                     panel_grid_major_y=element_line(
                        color=axisColor, linetype='solid', size=1),
                     panel_grid_major_x=element_blank(),
                     figure_size=(7.0, 4.4),
                     title=element_text(weight="bold", size=font_size, 
                                        family=font, color=blackTitle),
                     axis_title=element_text(family=labelFont, size=12, color=guideTitleColor),
                     axis_text=element_text(family=labelFont, size=10, color=guideLabelColor, 
                                            margin={'r': 4}
                                           ),
                     axis_title_x=element_text(margin={'t': 10}),
                     axis_title_y=element_text(margin={'r': 10}),
                     legend_title=element_text(font=labelFont, size=14, color=blackTitle, 
                                               margin={'b': 10}),
                     legend_text=element_text(font=labelFont, size=11, color=blackTitle, 
                                              margin={'t': 5, 'b': 5, 'r': 5, 'l': 5}),
                    )
            )    
    #ax1 = preset_plotnine_config(ax1)
    return ax1
  
    
    

# Tiffany's function for changing labels

def labeling(word):
    # Add specific use cases where it's not just first letter capitalized
    LABEL_DICT = { "prepared_y": "Year",
              "dist": "District",
              "total_requested": "Total Requested",
              "fed_requested":"Fed Requested",
              "ac_requested": "Advance Construction Requested",
              "nunique":"Number of Unique",
              "project_no": "Project Number"}
    
    if (word == "mpo") or (word == "rtpa"):
        word = word.upper()
    elif word in LABEL_DICT.keys():
        word = LABEL_DICT[word]
    else:
        word = word.replace('n_', 'Number of ').title()
        word = word.replace('unique_', "Number of Unique ").title()
        word = word.replace('_', ' ').title()
    
    return word


#basic charting to get number of unique values in column

def basic_agg_nunique(df, col, aggregate_by=["dist"]):
    df1 = ((df >> group_by(_[aggregate_by]) >> summarize(n=_[col].nunique()) >> arrange(-_.n)).head(20))
    chart = (alt.Chart(df1)
             .mark_bar()
             .encode(
                 x=alt.X(aggregate_by, title=f"{labeling(aggregate_by)}"),
                 y=alt.Y("n", title=f"Number of Unique {labeling(col)}"),
                 #column = "payment:N",
                 color = alt.Color("n",
                                  scale=alt.Scale(
                                      range=altair_utils.CALITP_SEQUENTIAL_COLORS),
                                      legend=alt.Legend(title=f"{(labeling(col))}")
                                  ))
             .properties( 
                          title=f"Number of Unique {labeling(col)} by {labeling(aggregate_by)}")
    )


    chart.save(f"./chart_outputs/{col}_by_{aggregate_by}.png")

    return chart


#chart with choice of aggregation method

def chart_top_agg(df, col, aggregate_by, aggfunc):
    
    #df1 = (calculate_data_head(df, col, aggregate_by, aggfunc))
    
    chart = (alt.Chart(df1)
             .mark_bar()
             .encode(
                 x=alt.X(aggregate_by, title=labeling(aggregate_by), sort=('-y')),
                 y=alt.Y(col, title=labeling(col)),
                 #column = "payment:N",
                 color = alt.Color(col,
                                  scale=alt.Scale(
                                      range=altair_utils.CALITP_SEQUENTIAL_COLORS),
                                      legend=alt.Legend(title=(labeling(col)))
                                  ))
             .properties( 
                          title=f"Highest {labeling(col)} {labeling(aggfunc)}s by {labeling(aggregate_by)}")
    )


    chart.save(f"./chart_outputs/{col}_{aggfunc}_by_{aggregate_by}.png")
    
    return chart



#group charting: examples
    #groupby_col_x axis = prepared_y
    #agg_by_col = dist or mpo 
    #sum_col= primary_agency_name or prefix

def group_chart_nunique(df, groupby_col_x, agg_by_col, sum_col):
    dist_years1 = (df >> group_by(_[groupby_col_x], _[agg_by_col]) 
                   >> summarize(n=_[sum_col].nunique()) 
                   >> arrange(-_[groupby_col_x]))

    
    
    chart = alt.Chart(dist_years1).mark_bar().encode(
            column=((f"{agg_by_col}:N")
                    #, title=labeling(agg_by_col)
                   ),
            x=alt.X((f"{groupby_col_x}:O"), title=labeling(groupby_col_x)),
            y=alt.Y('n:Q', title=(f"Number of Unique {labeling(sum_col)}")),
            color = alt.Color((f"{agg_by_col}:N"), 
                                  scale=alt.Scale(
                                      range=altair_utils.CALITP_SEQUENTIAL_COLORS),  
                                   legend=alt.Legend(title=(labeling(sum_col)))
                                  )
                                  )
    chart.save(f"./chart_outputs/grouped_{agg_by_col}_by_{groupby_col_x}.png")
    
    return chart




