"""
Utils for Scheduled vs RT Trips
"""

import os
os.environ["CALITP_BQ_MAX_BYTES"] = str(800_000_000_000) ## 800GB?

import altair as alt
import branca

import gcsfs
import pandas as pd

from siuba import *
from calitp_data_analysis.tables import tbls
from calitp_data_analysis.sql import query_sql
import calitp_data_analysis.magics

from shared_utils import portfolio_utils
from calitp_data_analysis import geography_utils, styleguide
from calitp_data_analysis import calitp_color_palette as cp
from dla_utils import _dla_utils as dla_utils

fs = gcsfs.GCSFileSystem()

# Read in complete data table
def read_data():
    
    df = query_sql(
    """
    SELECT *
    FROM `cal-itp-data-infra.views.gtfs_rt_vs_schedule_trips_sample`
    """
    )
    
    df['service_date'] = pd.to_datetime(df['service_date'])
    df['weekday'] = pd.Series(df.service_date).dt.day_name()
    df['month'] =  pd.Series(df.service_date).dt.month_name()
    
    #filtering for now to avoid the expired calitp_urls
    df = df[((df["calitp_itp_id"]==290) & (df["calitp_url_number"]==1)) | ((df["calitp_itp_id"]==300))]
    
    # itpid_district = portfolio_utils.add_caltrans_district()
    df = pd.merge(df, itpid_district, on='calitp_itp_id', how='left')
    
    assert len(df>>filter(_.agency_name.isnull())) == 0, "PASS"

    return df

def get_correct_url(df):
    
    all_multiple = (df>>group_by(_.calitp_itp_id, _.agency_name)>>summarize(n_itp_url = _.calitp_url_number.nunique()) >>filter(_.n_itp_url>1))
    
    all_multiple_agencies = all_multiple.calitp_itp_id.to_list()
    
    df['multiple_url'] = df['calitp_itp_id'].isin(all_multiple_agencies)

    def remove_rows(row):
        
        #Emery Go-Round
        if (row.multiple_url == True) and (row.calitp_itp_id == 106) and (row.calitp_url_number == 2):
            return "drop"        
        
        #SMART or Sonoma Marin Area Rail Transit
        if (row.multiple_url == True) and (row.calitp_itp_id == 315) and (row.calitp_url_number == 2):
            return "drop"
        
        #  Union City Transit
        elif (row.multiple_url == True) and (row.calitp_itp_id == 350) and (row.calitp_url_number == 2):
            return "drop"
        
        # Victor Valley Transit Authority
        elif (row.multiple_url == True) and (row.calitp_itp_id == 360) and (row.calitp_url_number == 1):
            return "drop"
        
        # MTS has two agency names for itp id
        elif (row.calitp_itp_id == 278) and (row.agency_name == ('San Diego International Airport')):
            return "drop"
        
        # drop others with only two: 
        elif (row.multiple_url == True) and (row.calitp_url_number == 0):
            return "drop"


    df['drop_record'] = df.apply(lambda x: remove_rows(x), axis=1)
    
    ##commenting 106 out to maintain 511 MTC
    #df.loc[((df['calitp_itp_id']==106) & (df['calitp_url_number'] == 1)), 'drop_record']  = ""
    df.loc[((df['calitp_itp_id']==360) & (df['calitp_url_number'] == 0)), 'drop_record']  = ""
    
    df = df >> filter(_.drop_record!='drop')
    df = df.drop(columns = ['multiple_url','drop_record'])
    
    return df

# # Get the data for Scheduled Trips and RT Trips long format
# def load_schedule_data(start_date, end_date, itp_id):
#     gtfs_daily = (
#         tbl.views.gtfs_schedule_fact_daily_trips()
#         >> filter(_.calitp_itp_id == itp_id)
#         >> filter((_.service_date >= start_date),
#                  (_.service_date <= end_date))
#         >> filter(_.is_in_service == True)
#         >> collect()
#     )
#     return gtfs_daily

# def load_rt_data(start_date, end_date):
#     rt = query_sql(
#         f"""
#         SELECT * FROM `cal-itp-data-infra-staging.natalie_views.gtfs_rt_distinct_trips`
#         WHERE date BETWEEN '{start_date}' AND '{end_date}'
#         """
#     )
#     return rt

# # Get a DF with Percent Ran for each Day long format

# def get_pct_ran_df(itp_id, list_of_dates, gtfs_daily, rt):
    
#     pcts = []
    
#     # loop through list of dates
#     for single_date in list_of_dates:
        
#         #filter for single day
        
#         gtfs_daily2 = (gtfs_daily>>filter(_.service_date == single_date))
#         rt2 = (rt>>filter(_.date == single_date))
        
#         #outer join schedule and rt data 
#         sched_rt_df = (pd.merge(gtfs_daily2, rt2, how='outer', on='trip_id', indicator='have_rt'))

        
#         day_pct_ran = {}
#         day_pct_ran['date'] = single_date
#         if ((len(sched_rt_df))!=0):
#             day_pct_ran['pct_trips_ran'] = ((len(sched_rt_df>>filter(_.have_rt=='both')))/(len(gtfs_daily2)))
#         elif ((len(sched_rt_df))==0):
#             day_pct_ran['pct_trips_ran'] = ''
#         pct_ran = pd.DataFrame([day_pct_ran])
        
#         # add columns with counts 
#         pct_ran['n_have_rt'] = (len(sched_rt_df>>filter(_.have_rt=='both')))
#         pct_ran['n_missing_rt'] = (len(sched_rt_df>>filter(_.have_rt=='right_only')))
#         pct_ran['unmatched_rt'] = (len(sched_rt_df>>filter(_.have_rt=='left_only')))
        
#         # add columns for number of unique trip_ids
#         pct_ran['nunique_sched'] = (gtfs_daily2.trip_id.nunique())
#         pct_ran['nunique_rt'] = (rt2.trip_id.nunique())

#         pcts.append(pct_ran)                                                    
#         #code help from: https://stackoverflow.com/questions/28669482/appending-pandas-dataframes-generated-in-a-for-loop
   
#     #add each date together 
#     pcts = pd.concat(pcts)
    
#     #arrange by date
#     pcts = pcts>>arrange(_.date)
#     return pd.DataFrame(pcts)

#can be retired
def agg_by_date(df, sum1_sched, sum2_vp):
    agg_df = (df
     >>group_by(_.calitp_itp_id,
                _.agency_name,
            _.calitp_url_number,
            _.service_date,
            _.weekday,
           _.month)
     >>summarize(total_num_sched = (_[sum1_sched].sum()),
             total_num_vp = (_[sum2_vp].sum()))
     >>mutate(pct_w_vp = (_.total_num_vp)/(_.total_num_sched))
            )
    return agg_df

#same as agg_by_date but allows more flexibility in group columns
def get_agg_pct(df,
                groupings: list,
                sum_sched: list,
                sum_vp: list,
               ):
    
    agg_df = (portfolio_utils.aggregate_by_geography(df,
                                       group_cols = groupings,
                                       sum_cols = [sum_sched, sum_vp]
                                       ))>>mutate(avg = _[sum_vp]/_[sum_sched])
    return agg_df



def groupby_onecol(df, groupbycol, aggcol):
    if groupbycol == "weekday":
        cats_day = ['Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday', 'Sunday']
        grouped = df>>group_by(_[groupbycol])>>summarize(avg = _[aggcol].mean())
        grouped['weekday'] = pd.Categorical(grouped['weekday'], categories=cats_day, ordered=True)
        grouped = grouped.sort_values('weekday')
        return grouped
    
    if groupbycol== "month":
        cats_month = ['January', 'February', 'March', 'April', 'May', 'June', 'July', 'August', 'September', 'October', 'November', 'December']
        grouped = df>>group_by(_[groupbycol])>>summarize(avg = _[aggcol].mean())
        grouped['month'] = pd.Categorical(grouped['month'], categories=cats_month, ordered=True)
        grouped = grouped.sort_values('month')
        return grouped

    else:
        grouped2 = df>>group_by(_[groupbycol])>>summarize(avg = _[aggcol].mean())
        return grouped2


def groupby_twocol(df, groupbycol1, groupbycol2, aggcol, timeframe):
    if timeframe == "weekday":
        cats_day = ['Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday', 'Sunday']
        grouped = df>>group_by(_[groupbycol1], _[groupbycol2])>>summarize(avg = _[aggcol].mean())
        grouped['weekday'] = pd.Categorical(grouped['weekday'], categories=cats_day, ordered=True)
        grouped = grouped.sort_values('weekday')
        return grouped
    
    if timeframe== "month":
        cats_month = ['January', 'February', 'March', 'April', 'May', 'June', 'July', 'August', 'September', 'October', 'November', 'December']
        grouped = df>>group_by(_[groupbycol1], _[groupbycol2])>>summarize(avg = _[aggcol].mean())
        grouped['month'] = pd.Categorical(grouped['month'], categories=cats_month, ordered=True)
        grouped = grouped.sort_values('month')
        return grouped

    elif timeframe == "":
        grouped2 = df>>group_by(_[groupbycol1], _[groupbycol2])>>summarize(avg = _[aggcol].mean())
        return grouped2
    
def labeling(word):
    # Add specific use cases where it's not just first letter capitalized
    LABEL_DICT = {
        "dist": "District",
        "nunique": "Number of Unique",
        "n":"Count",
        "pct_w_vp": "Percent of Scheduled Trips with Vehicle Postions",
        "avg_pct_w_vp":"Average Percent of Scheduled Trips with Vehicle Positions",
        "primary_agency_name": "Agency",
        "avg":"Average",
        "num_vp":"Number of Trips with Vehicle Positions Data",
        "num_sched":"Number of Trips with Scheduled Data"
    }

    if (word == "mpo") or (word == "rtpa"):
        word = word.upper()
    elif word in LABEL_DICT.keys():
        word = LABEL_DICT[word]
    else:
        word = word.replace("n_", "Number of ").title()
        word = word.replace("unique_", "Number of Unique ").title()
        word = word.replace("_", " ").title()
        word = word.title()

    return word

## Charting Functions 

#adding selection and highlight for charts
# allows users to interact with chart and look at a singular line
def add_chart_selection(chart, selection1):
    
    selection = (alt.selection_multi(fields=[selection1], bind='legend'))
    
    chart = chart.encode(opacity=alt.condition(selection, alt.value(1), alt.value(0))).add_selection(selection)  
    
    return chart

## working on this still
# def add_chart_highlight(chart, highlight1):
    
#     highlight = (alt.selection(type = 'single', on='mouseover',
#                                fields=[highlight1], nearest=True))
    
#     chart = chart.encode(size=alt.condition(~highlight, alt.value(2), alt.value(5))).add_selection(highlight)
    
#     return chart

#adding tooltip for three options
def add_tooltip(chart, tooltip1, tooltip2, tooltip3):
    chart=chart.encode([alt.Tooltip(tooltip1, title=labeling(tooltip1)),
                        alt.Tooltip(tooltip2, title=labeling(tooltip2)),
                        alt.Tooltip(tooltip2, title=labeling(tooltip2))])
    return chart

# Bar chart over time 
#need to specify color scheme outside of charting function which can be done with .encode()
def bar_chart_over_time(df,
                        x_col,
                        y_col,
                        color_col,
                        yaxis_format,
                        sort,
                        title_txt):
    
    bar = (alt.Chart(df)
        .mark_bar(size=8)
        .encode(
            x=alt.X(x_col, title=labeling(x_col), sort=(sort)),
            y=alt.Y(y_col, title=labeling(y_col), axis=alt.Axis(format=yaxis_format)),
            color = color_col,
        )
           # .properties(title=title_txt)
          )
    
    #chart = styleguide.preset_chart_config(bar)
    chart = dla_utils.add_tooltip(bar, x_col, y_col)
    return (chart.properties(width=550))

#easy chart for full average 
def total_average_chart(full_df):
    
    # get the average 
    agg_all = ((get_agg_pct(full_df,
                  groupings = ['service_date'],
                  sum_sched = 'num_sched',
                  sum_vp = 'num_vp'))>>arrange(_.service_date)).rename(columns={'avg':'total_average'})
      
    base = (alt.Chart(agg_all).properties(width=550))

    chart = (base.mark_line().encode(x=alt.X('service_date:O', title=labeling('service_date'), sort=("x")),
                                     y=alt.Y('avg:Q', title= labeling('avg'), axis=alt.Axis(format='%')),
                                     ))
             # .properties(title= 'Overall Average for Percent Trips with Vehicle Positions Data')
           
    return chart

#chart for Single operator vs total average
def total_average_with_1op_chart(full_df, calitp_id):
    
    agg_all = ((get_agg_pct(full_df,
                  groupings = ['service_date'],
                  sum_sched = 'num_sched',
                  sum_vp = 'num_vp'))>>arrange(_.service_date)).rename(columns={'avg':'total_average'})
    
    one_op = (get_agg_pct((full_df>>filter(_.calitp_itp_id==calitp_id)),
                  groupings = ['service_date', 'agency_name'],
                  sum_sched = 'num_sched',
                  sum_vp = 'num_vp'))>>arrange(_.service_date)
    
    one_op = one_op.rename(columns={'avg':f'{(one_op.iloc[0]["agency_name"])} Average'})
    
    
    by_date = pd.merge(agg_all, one_op, on= 'service_date', how='left')
    by_date = by_date.rename(columns={'total_average':'Total Average'})
    
    by_date_long =  (by_date >>select(_.service_date,
                                 _['Total Average'],
                                  _[f'{(one_op.iloc[0]["agency_name"])} Average'])
                 >>gather('measure',
                                   'value',
                                   _['Total Average'],
                                   _[f'{(one_op.iloc[0]["agency_name"])} Average']))
    by_date_long = by_date_long.rename(columns={'measure':'Averages',
                                           'value':'Percent with Vehicle Position Data'})
    
    chart = (bar_chart_over_time(by_date_long, 
                           'service_date', 
                           'Percent with Vehicle Position Data', 
                           'Averages',
                           '%', 
                           "x", 
                           '')).mark_line()
             # .properties(
        # title=(f'{(one_op.iloc[0]["agency_name"])} Average Compared to Overall Average'))
    return chart.properties(width=550)
