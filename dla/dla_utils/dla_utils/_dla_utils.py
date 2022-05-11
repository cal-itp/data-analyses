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


from IPython.display import Markdown, HTML
from IPython.core.display import display

from shared_utils import altair_utils
from shared_utils import geography_utils
from shared_utils import calitp_color_palette as cp
from shared_utils import styleguide

from calitp import to_snakecase
import intake

import ipywidgets as widgets
from ipywidgets import *




pd.options.display.float_format = '{:,.2f}'.format

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


def count_all_years(df, groupedby=["prepared_y", "dist"]):
    count_years = (
        geography_utils.aggregate_by_geography(
            df,
            group_cols=groupedby,
            sum_cols=[
                "adjusted_total_requested",
                "adjusted_ac_requested",
                "adjusted_fed_requested",
            ],
            mean_cols=[
                "adjusted_total_requested",
                "adjusted_ac_requested",
                "adjusted_fed_requested",
            ],
            nunique_cols=[
                "primary_agency_name",
                "mpo",
                "prefix",
                "project_no",
                "project_location",
                "type_of_work",
            ],
            rename_cols=False
        )
        .sort_values(["prepared_y", "dist"], ascending=[False, True])
        .astype({"prepared_y": "Int64"})
    )

    count_years = count_years.rename(
        columns={
            "ac_requested_x": "ac_requested_sum",
            "fed_requested_x": "fed_requested_sum",
            "total_requested_x": "total_requested_sum",
            "ac_requested_y": "ac_requested_mean",
            "fed_requested_y": "fed_requested_mean",
            "total_requested_y": "total_requested_mean",
            "prefix": "unique_prefix",
            "primary_agency_name": "unique_primary_agency_name",
            "mpo": "unique_mpo",
            "project_location": "unique_project_location",
            "project_no": "unique_project_no",
            "type_of_work": "unique_type_of_work",
        }
    )
    count_years = count_years.dropna(axis=0)

    return count_years

## want to create a function that gets a new dataframe that will hold all the counts and top twenty 
def find_top(df):
    
    cols= ["prefix",
           "prepared_y",
           "status_comment",
           "project_location",
           "type_of_work",
           "seq",
           "mpo",
           "primary_agency_name",
           "dist"]
    
    RENAME_DICT = {}
    
    def count_and_sort(col):
        df1 = ((df >> count(_[col]) >> arrange(-_.n))
        .head(20).reset_index(drop=True)
               .assign(variable=col)
               .rename(columns = {col:'value', "n":"count"})
        )
        
        return df1
    
    final = pd.DataFrame()
    for col in cols:
        agg = count_and_sort(col)
        
        final = pd.concat([final, agg], sort = False, axis=0)
    
    
    return final




def get_nunique(df, col, groupby_col):
    
    counts = (df >> group_by(_[groupby_col]) >> summarize(n=_[col].nunique()) >> arrange(-_.n))
    
#     if groupby_col == []:
#         counts = (df >> group_by(_[groupby_col]) >> summarize(n=_[col].nunique()) >> arrange(-_.n))

        
#     elif groupby_col != []:
#         counts = (df >> summarize(n=_[col].nunique()) >> arrange(-_.n))
    
    return counts



def project_cat(df, i):
    subset = df >> filter(_[i] == 1)
    subset_2 = (
        (find_top(subset))
        >> filter(_.variable == "primary_agency_name")
        >> select(_.value, _.count)
    ).head(5)
    subset_2["Percent of Category"] = ((subset_2["count"]) / (len(subset))) * 100
    subset_2 = subset_2.rename(
        columns={"value": "Agency", "count": f"{labeling(i)} Obligations"}
    )

    # generate chart:

    subset_3 = (
        (
            subset.groupby(["primary_agency_name"])
            .agg(
                {
                    i: "sum",
                    "process_days": "mean",
                    "adjusted_total_requested": "mean",
                    "adjusted_fed_requested": "mean",
                    "adjusted_ac_requested": "mean",
                }
            )
            .reset_index()
        )
        >> arrange(-_[i])
    ).head(5)

    subset_3 = subset_3.rename(
        columns={
            "primary_agency_name": "Agency",
            "adjusted_total_requested": "Total Requested",
            "adjusted_fed_requested": "Fed Requested",
            "adjusted_ac_requested": "AC Requested",
        }
    )

    subset_4 = pd.melt(
        subset_3,
        id_vars=["Agency"],
        value_vars=["Total Requested", "Fed Requested", "AC Requested"],
        var_name="Categories",
        value_name="Funding Amount",
    )

    chart = (
        alt.Chart(subset_4)
        .mark_bar()
        .encode(
            x=alt.X(
                "Funding Amount",
                axis=alt.Axis(format="$.2s", title="Obligated Funding ($2021)"),
            ),
            y=alt.Y("Agency"),
            color=alt.Color(
                "Categories:N",
                scale=alt.Scale(range=altair_utils.CALITP_CATEGORY_BRIGHT_COLORS),
            ),
            row="Categories:N",
        )
    )

    chart = add_tooltip(chart, "Agency", "Funding Amount")
    display(HTML(f"<h3>Top Agencies using {labeling(i)} Projects</h3>"))
    display(subset_2.style.format(formatter={("Percent of Category"): "{:.2f}%"}))
    #display(chart)
   
    

"""
Labeling
"""

# Tiffany's function for changing labels

def labeling(word):
    # Add specific use cases where it's not just first letter capitalized
    LABEL_DICT = {
        "prepared_y": "Year",
        "dist": "District",
        "total_requested": "Total Requested",
        "fed_requested": "Fed Requested",
        "ac_requested": "Advance Construction Requested",
        "nunique": "Number of Unique",
        "project_no": "Project Number",
        "active_transp": "Active Transportation",
        "infra_resiliency_er": "Infrastructure & Emergency Relief",
        "congestion_relief": "Congestion Relief",
        "n":"Count",
        "primary_agency_name":"Agency"
    }

    if (word == "mpo") or (word == "rtpa"):
        word = word.upper()
    elif word in LABEL_DICT.keys():
        word = LABEL_DICT[word]
    else:
        #word = word.replace("n_", "Number of ").title()
        word = word.replace("unique_", "Number of Unique ").title()
        word = word.replace("_", " ").title()

    return word



def add_tooltip(chart, tooltip1, tooltip2):
    chart = (
        chart.encode(tooltip= [tooltip1,tooltip2]))
    return chart




"""
Basic Charts
"""

# %%html
# <style>
# @import url('https://fonts.googleapis.com/css?family=Lato');
# </style>



# Bar
def basic_bar_chart(df, x_col, y_col, color_col, subset, chart_title=''):
    
    if chart_title == "":
        chart_title = (f"{labeling(x_col)} by {labeling(y_col)}")
    
    chart = (alt.Chart(df)
             .mark_bar()
             .encode(
                 x=alt.X(x_col, title=labeling(x_col), sort=('-y')),
                 y=alt.Y(y_col, title=labeling(y_col)),
                 #column = "payment:N",
                 color = alt.Color(color_col,
                                  scale=alt.Scale(
                                      range=altair_utils.CALITP_SEQUENTIAL_COLORS),
                                      legend=alt.Legend(title=(labeling(color_col)))
                                  ))
             .properties( 
                          title=chart_title)
    )

    chart=styleguide.preset_chart_config(chart)
    savepath = (chart_title.replace(" ", "_"))
#    chart.save(f"./chart_outputs/d{subset}_outputs/bar_{savepath}.png")
    
    return chart



def basic_bar_chart_no_save(df, x_col, y_col, color_col, subset
                           # , chart_title=''
                           ):

    # if chart_title == "":
    #     chart_title = (f"{labeling(x_col)} by {labeling(y_col)}")

    chart = (alt.Chart(df)
             .mark_bar()
             .encode(
                 x=alt.X(x_col, title=labeling(x_col), sort=('-y')),
                 y=alt.Y(y_col, title=labeling(y_col)),
                 color = alt.Color(color_col,
                                  scale=alt.Scale(
                                      range=altair_utils.CALITP_CATEGORY_BRIGHT_COLORS),
                                      legend=alt.Legend(title=(labeling(color_col)), symbolLimit=10)
                                  )
                                   )
             # .properties( 
             #              title=chart_title)
            )

    chart=styleguide.preset_chart_config(chart)
    chart = add_tooltip(chart, x_col, y_col)
    return chart


# Scatter 
def basic_scatter_chart(df, x_col, y_col, color_col, subset, chart_title=""):
    
    if chart_title == "":
        chart_title = (f"{labeling(x_col)} by {labeling(y_col)}")
        
    chart = (alt.Chart(df)
             .mark_circle(size=60)
             .encode(
                 x=alt.X(x_col, title=labeling(x_col)),
                 y=alt.Y(y_col, title=labeling(y_col)),
                 color = alt.Color(color_col,
                                  scale=alt.Scale(
                                      range=altair_utils.CALITP_SEQUENTIAL_COLORS),
                                      legend=alt.Legend(title=(labeling(color_col)))
                                  ))
             .properties( 
                          title = (chart_title))
    )

    chart=styleguide.preset_chart_config(chart)
    savepath = (chart_title.replace(" ", "_"))
    chart.save(f"./chart_outputs/d{subset}_outputs/scatter_{savepath}.png")
    
    
    return chart




# Line
def basic_line_chart(df, x_col, y_col, subset, chart_title=''):
    
    if chart_title == "":
        chart_title = (f"{labeling(x_col)} by {labeling(y_col)}")
    
    
    chart = (alt.Chart(df)
             .mark_line()
             .encode(
                 x=alt.X(x_col, title=labeling(x_col)),
                 y=alt.Y(y_col, title=labeling(y_col))
                                   )
              ).properties( 
                          title=chart_title)

    chart=styleguide.preset_chart_config(chart)
    # savepath = (chart_title.replace(" ", "_"))
    # chart.save(f"./chart_outputs/d{subset}_outputs/line_{savepath}.png")
    
    return chart


def basic_line_chart_test_no_save(df, x_col, y_col, subset
                                  # , chart_title=''
                                 ):
    
    # if chart_title == "":
    #     chart_title = (f"{labeling(x_col)} by {labeling(y_col)}")
     
    chart = (alt.Chart(df)
             .mark_line()
             .encode(
                 x=alt.X(x_col, title=labeling(x_col)),
                 y=alt.Y(y_col, title=labeling(y_col)))
              )
    # .properties( 
    #                       title=chart_title)

    chart=styleguide.preset_chart_config(chart)
    chart = add_tooltip(chart, x_col, y_col)
    return chart


"""
Interactive Functions
"""

## From Tiffany's DLA Function Branch on Github

def summarize_and_plot(df, select_col, place):
    subset = df[df[select_col]==place].rename(
        columns = {
            "fed_requested": "Federal",
            "ac_requested": "AC",
            "total_requested": "Total",
        }
    )
    
    prefix_count_n = subset >> count(_.prefix)

    display(Markdown(f"**Summary Statistics for {place}**"))
    display(Markdown(f"The number of obligations {place} has is {len(prefix_count_n)}"))
    
    display(Markdown(
        f"The number of prefix codes {place} uses is {subset.prefix.nunique()}"))

    pd.set_option("display.max_columns", None)

    funds = subset[['Federal','AC','Total']].describe()
    display(funds.style.format(precision=2, na_rep='MISSING', thousands=","))

    display(Markdown(f"**Top Project Types in {place}**"))

    work_df = subset >> count(_.type_of_work) >> arrange(-_.n)
    display(work_df.head(5))
    
    ax1 = (prefix_count_n
            >> ggplot(aes("prefix", "n", fill="prefix")) 
               + geom_col() 
               + theme(axis_text_x = element_text(angle = 45 , hjust=1))
               + labs(title='Agency Program Codes', x='Program Codes', 
                      y='Number of Obligations', fill="Program Type")
        )
    display(ax1)

def on_selection(*args):
    output.clear_output()
    with output:
        summarize_and_plot(df, select_col, dropdown.value)


def interactive_widget(df, select_col):

    dropdown = widgets.Dropdown(
        description=f"{select_col.title()}",
        options=df[select_col].sort_values().unique().tolist(),
    )
    output = widgets.Output()

    display(dropdown)
    display(output)

    def on_selection(*args):
        output.clear_output()
        with output:
            summarize_and_plot(df, select_col, dropdown.value)

    dropdown.observe(on_selection, names="value")
    on_selection()


def interactive_widget_counts(df, select_col, unique_col):

    dropdown = widgets.Dropdown(
        description=f"{select_col.title()}",
        options=df[select_col].sort_values().unique().tolist(),
    )
    output = widgets.Output()

    display(dropdown)
    display(output)
    
    def counts(df, select_col, unique_col, place):
        subset = df[df[select_col]==place]
        counts = (subset >> count(_[unique_col]) >> arrange(-_.n))
    
        display(counts)


    def on_selection(*args):
        output.clear_output()
        with output:
            counts(df, select_col, unique_col, dropdown.value)

    dropdown.observe(on_selection, names="value")
    on_selection()