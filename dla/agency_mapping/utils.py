'''
Utils for IIJA Project Descriptions and Information:

Functions here to be used in cleaning script that will 
- add known names to organizations
- add project types that classify project type
- create a public-friendly project title 
'''

import numpy as np
import pandas as pd
from siuba import *

from shared_utils import geography_utils
from dla_utils import _dla_utils

from calitp import to_snakecase

import nltk
from nltk.corpus import stopwords
from nltk.tokenize import word_tokenize, sent_tokenize

import re

#function to add locodes

def add_name_from_locode(df, df_locode_extract_col):
    #read in locode sheet
    locodes = to_snakecase(pd.read_excel(f"gs://calitp-analytics-data/data-analyses/dla/e-76Obligated/locodes_updated7122021.xlsx"))
    
    #extract locode, make sure it i has the same spots
    df['locode'] = df[df_locode_extract_col].apply(lambda x: x[1:5])
    #make sure locode column is numeric
    df['locode'] = pd.to_numeric(df['locode'], errors='coerce')
    
    #merge
    df_all = (pd.merge(df, locodes, left_on='locode', right_on='agency_locode', how='left'))
    
    df_all = df_all.rename(columns={'agency_name':'implementing_agency',
                                   'locode':'implementing_agency_locode'})
    
    #if we use other locode list then drop these columns
    df_all.drop(columns =['active_e76s______7_12_2021_', 'mpo_locode_fads', 'agency_locode'], axis=1, inplace=True)
    
    return df_all


def title_column_names(df):
    df.columns = df.columns.map(str.title) 
    df.columns = df.columns.map(lambda x : x.replace("_", " "))
    
    return df



def tokenize(texts):
    return [nltk.tokenize.word_tokenize(t) for t in texts]


def get_list_of_words(df, col):
    nltk.download('stopwords')
    nltk.download('punkt')
    
    #get just the one col
    column = df[[col]]
    #remove single-dimensional entries from the shape of an array
    col_text = column.squeeze()
    # get list of words
    text_list = col_text.tolist()
    #join list of words 
    text_list = ' '.join(text_list).lower()
    
    # remove punctuation 
    text_list = re.sub(r'[^\w\s]','',text_list)
    swords = [re.sub(r"[^A-z\s]", "", sword) for sword in stopwords.words('english')]
    # remove stopwords
    clean_text_list = [word for word in word_tokenize(text_list.lower()) if word not in swords] 
    # turn into a dataframe
    clean_text_list = pd.DataFrame(np.array(clean_text_list))

    return clean_text_list




def add_description(df, col):
    ##using np.where. code help: https://stackoverflow.com/questions/43905930/conditional-if-statement-if-value-in-row-contains-string-set-another-column
    ## method for project in first column
    df['project_method'] = (np.where(df[col].str.contains("INSTALL"), "Install",
                        np.where(df[col].str.contains("CONSTRUCT"), "Construct",
                        np.where(df[col].str.contains("UPGRADE"), "Upgrade",
                        np.where(df[col].str.contains("IMPROVE"), "Improve",
                        np.where(df[col].str.contains("REPAIR"), "Repair",
                        np.where(df[col].str.contains("REPLACE"), "Replace",
                        np.where(df[col].str.contains("REPLACE ")& df[col].str.contains("BRIDGE"), "",
                        np.where(df[col].str.contains("REPLACE")& df[col].str.contains("GUARDRAIL"), "Replace",
                        np.where(df[col].str.contains("PAVE")| df[col].str.contains("PAVING"), "Pave",
                        np.where(df[col].str.contains("NEW"), "New",
                        
                                    "")))))))))))
    
    ## types of projects in second column
    df['project_type'] = (
                        #np.where(df.col.str.contains("BRIDGE REPLACEMENT") , "Bridge Replacement",
                        np.where(df[col].str.contains("SHOULDER"), "Shoulders",
                        np.where(df[col].str.contains("SYNCHRONIZE CORRIDOR"), "Synchronize Corridor",
                        np.where(df[col].str.contains("COMPLETE STREET"), "Complete Streets",
                        np.where(df[col].str.contains("BRIDGE PREVENTIVE MAINTENANCE"), "Bridge Preventive Maintenance",
                        np.where(df[col].str.contains("SIDEWALK"), "Sidewalk",
                        np.where(df[col].str.contains("SCOUR"), "Erosion Countermeasures",
                        np.where(df[col].str.contains("ROUNDABOUT"), "Roundabout",
                        np.where(df[col].str.contains("GUARDRAI"), "Guardrails", ##removing the "L"from Guardrail in case the word is cut off
                        np.where(df[col].str.contains("VIDEO DETECTION EQUIPMENT"), "Video Detection Equipment",
                        np.where(df[col].str.contains("PEDESTRIAN") & df[col].str.contains("BIKE") , "Pedestrian  & Bike Safety Improvements",
                        np.where(df[col].str.contains("BRIDGE") & df[col].str.contains("REHAB") , "Bridge Rehabilitation",
                        np.where(df[col].str.contains("PAVEMENT") & df[col].str.contains("REHAB") , "Pavement Rehabilitation",
                        np.where(df[col].str.contains("PEDESTRIAN"), "Pedestrian Safety Improvements",
                        np.where(df[col].str.contains("TRAFFIC SIG"), "Traffic Signals",
                        np.where(df[col].str.contains("BIKE SHARE"), "Bike Share Program",
                        np.where(df[col].str.contains("BIKE"), "Bike Lanes",                  
                        np.where(df[col].str.contains("SIGNAL"), "Signals",
                        np.where(df[col].str.contains("SIGN"), "Signage",
                        np.where(df[col].str.contains("BRIDGE"), "Bridge",
                        np.where(df[col].str.contains("SAFETY ") & df[col].str.contains("IMPROVE") , "Safety Improvemnts",
                                 'Project')
                                   ))))))))))))))))))))#)
    
    ## need to expand this to include more. maybe try a list. but capture entries with multiple projects
    df['other'] = (np.where(df[col].str.contains("CURB") & df[col].str.contains("SIDEWALK") | df[col].str.contains("BIKE"), "Multiple Road",
                                 "Other Projects"))
    
    return df


#function for getting title column

def add_new_title(df, first_col_method, second_col_type, third_col_name):
    """
    Function to add new title. 
    Expected output example: "New Bike Lane in Eureka"
    """
    #combining strings.
    df['project_name_new'] = df[first_col_method] + " " + df[second_col_type] + " in " + df[third_col_name]
    
    return df

'''
another approach (not as effective for creating new titles)
'''
## code help: https://stackoverflow.com/questions/70995812/extract-keyword-from-sentences-in-a-pandas-text-column-using-nltk-and-or-regex
def key_word_intersection(df, text_col):
    summaries = []
    for x in tokenize(df[text_col].to_numpy()):
        keywords = np.concatenate([
                                np.intersect1d(x, ['BRIDGE REPLACEMENT', 'BRIDGE', 'INSTALL', 'CONSTRUCT', 'REPLACE',
                                                   'SIGNAL', 'SIGNALS', 'TRAFFIC', 'IMPROVEMENT', 'PEDESTRIAN', 
                                                   'LANES', 'NEW', 'REHABILITATION','UPGRADE', 'CLASS',
                                                   'BIKE', 'WIDEN', 'LANDSCAPING', 'SAFETY', 'RAISED', 
                                                   'SEISMIC', 'SIGNAGE', 'RETROFIT', 'ADD', 'PLANNING', 'PAVE',
                                                   'PREVENTIVE','MAINTENANCE', 'REHAB', 'RESURFACE', 'REPAIR', 'ROUNDABOUT'
                                                  'COMPLETE STREET', 'VIDEO DETECTION EQUIPMENT', 'SYNCHRONIZE CORRIDOR', 'ROADWAY REALIGNMENTS']),
                                np.intersect1d(x, [
                                    # 'BRIDGE', 'ROAD', 'RD', 'AVENUE', 'AVE', 'STREET' , 'ST',
                                                   # 'FRACTURED', 'LANE', 'DRIVE', 'BOULEVARD', 'BLVD',
                                                   'INTERSECTION', 'INTERSECTIONS', 'SIDEWALK', 
                                    # 'WAY', 'DR', 'CURB', 'ROADWAY',
                                                   # 'TRAIL', 'PATH', 'CREEK', 'RIVER', 
                                    # 'CORRIDOR', 'CROSSING','PARKWAY','RAMPS', 'GUARDRAIL'
                                ]), 
                                np.intersect1d(x, ['CITY', 'COUNTY', 'STATE', 'UNINCORPORATED'])])
    
        summaries.append(np.array(x)[[i for i, keyword in enumerate(x) if keyword in keywords]])
    return summaries 