"""
Replica and Streetlight Analysis Utils
"""

import pandas as pd
from siuba import *
import ast

from calitp_data_analysis.sql import to_snakecase

import altair as alt
from calitp_data_analysis import calitp_color_palette as cp


"""
Replica Analysis Utils
"""
##function that returns Replica transit data into df we can analyze easier
def get_tranist_agency_counts(df, primary_mode_col, transit_mode_col, transit_agency_col, activity_id_col):
    ## return a df with the agency counts
    agencies = (df
         >>filter(_[primary_mode_col] =="public_transit")
         >>group_by(_[primary_mode_col], _[transit_mode_col], _[transit_agency_col])
         >>summarize(n =_[activity_id_col].nunique())
         >>arrange(-_.n))
    
    agencies[transit_mode_col] = agencies[transit_mode_col].astype(str)
    agencies[transit_agency_col] = agencies[transit_agency_col].astype(str)
    
    agencies['agency_count'] = [len(set(x.split(", "))) for x in
                        agencies[transit_agency_col].str.lower()]
    agencies['n_modes_taken'] = agencies[transit_mode_col].apply(lambda x: len(x.split()))
    
    ## return a df with the mode counts
    modes = (df
             >>filter(_[primary_mode_col] =="public_transit")
             >>count(_[transit_mode_col])>>arrange(-_.n))
    modes['n_modes_taken'] = modes[transit_mode_col].apply(lambda x: len(x.split()))

    return agencies, modes

def get_list_of_agencies(df, transit_agency_col):
    
    ## Get just one columns
    column = df[[transit_agency_col]]
    #remove single-dimensional entries from the shape of an array
    col_text = column.squeeze()
    # get list of words
    text_list = col_text.tolist()
    # #join list of words 
    text_list = ', '.join(text_list).title()
    
    text_list = text_list.replace(", ", "', '")
    text_list = "['" + text_list + "']"
    
    agency_list = ast.literal_eval(text_list)
    agency_list = set(agency_list)
    
    return agency_list

def get_dummies_by_agency(df, col):
    transit_agencies = set()
    for agencies in df[col].str.split(', '):
        transit_agencies.update(agencies)
    unique_agencies = []
    
    for agency in transit_agencies:
        df[agency] = df[col].str.count(agency)
        unique_agencies.append(agency)

    ### adding column for unique agencies list
    def get_unique_agencies(agency_list):
        unique_agencies = set()
        for agencies in agency_list:
            unique_agencies.update(agencies.split(', '))
        return ', '.join(sorted(list(unique_agencies)))

    # Applying the function to each row of the dataframe to get unique agencies
    df['unique_agencies'] = df[col].str.split(', ').apply(lambda x: get_unique_agencies(x))
    
    return df

def get_agencies_occurances(df):
    
    ##get list of columns that have agency names
    columns_to_keep = list(df.columns.values)
    
    ## remove columns that are not agency names
    unwanted_cols = {'transit_agency', 'unique_agencies', 'primary_mode', 'transit_submode', 'n', 'agency_count', 'n_modes_taken' }
    columns_to_keep = [e for e in columns_to_keep if e not in unwanted_cols]
    
    df_agencies = df.loc[:, columns_to_keep]
    
    ## transpose the df again
    df_agencies = pd.DataFrame(df_agencies).transpose().reset_index().rename(columns={'index':'agency'})
    
    ##sum up the number of trips by agency
    df_agencies['n_trips'] = df_agencies[list(df_agencies.columns)].sum(axis=1)
    
    ## filter df to only have two cols
    df_agencies = df_agencies>>select(_.agency, _.n_trips)
    
    return df_agencies

"""
Streetlight Analysis Utils
"""


