import pandas as pd
from calitp import *
import A1_data_prep

from plotnine import *
import intake
from shared_utils import geography_utils

import altair as alt
import altair_saver
from shared_utils import geography_utils
from shared_utils import altair_utils
from shared_utils import calitp_color_palette as cp
from shared_utils import styleguide

"""
Summary table for ZEV
"""
# Format numbers to currency in uuu8
def currency_format(df, col_name: str):
    df[col_name] = "$" + (df[col_name].astype(float)).round(0).astype(str)
    return df

def zev_summary(
    df_zev,
    df_all_projects,
    group_by_cols: list,
    sum_cols: list,
    count_cols: list,
    monetary_cols: list,
):
    # Group by
    zev_summary = df_zev.groupby(group_by_cols).agg(
        {**{e: "sum" for e in sum_cols}, **{e: "count" for e in count_cols}}
    )

    zev_summary = zev_summary.reset_index()

    # Aggregate the original dataframe with ALL projects, even non ZEV in grant program
    all_projects = (
        df_all_projects.groupby(group_by_cols)
        .agg({**{e: "count" for e in count_cols}})
        .reset_index()
    )

    # Merge the summaries together to calculate % of zev projects out of total projects
    m1 = pd.merge(zev_summary, all_projects, how="inner", on=group_by_cols)

    # Get grand totals
    m1 = m1.append(m1.sum(numeric_only=True), ignore_index=True)

    # Format to currency
    m1 = A1_data_prep.currency_format(m1, monetary_cols)

    # Clean cols
    m1 = A1_data_prep.clean_up_columns(m1)

    return m1

'''
Charts Functions
'''
#Labels for charts 
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
        word = word.replace('unique_', "Number of Unique ").title()
        word = word.replace('_', ' ').title()
    
    return word

### Bar chart with interactive tooltip: x_col and y_col will show up ### 
### This function only returns a chart, doesn't save
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
                                      range=cp.CALITP_CATEGORY_BRIGHT_COLORS),
                                      legend=alt.Legend(title=(labeling(colorcol)))
                                  ),
                tooltip = [x_col, y_col])
             .properties( 
                       title=chart_title)
    )

    chart=styleguide.preset_chart_config(chart)
    chart.save(f"./bar_{chart_title}.png")
    return chart


