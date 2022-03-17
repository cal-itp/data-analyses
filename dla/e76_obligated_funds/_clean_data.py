"""
Cleaning DLA Data

Obligated List:
https://dot.ca.gov/programs/local-assistance/reports/e-76-obligated

Waiting List: 
https://dot.ca.gov/programs/local-assistance/reports/e-76-waiting


"""

#! pip install cpi

import numpy as np
import pandas as pd
from calitp import to_snakecase
from siuba import *
import cpi

from calitp.storage import get_fs
fs = get_fs()

GCS_FILE_PATH = 'gs://calitp-analytics-data/data-analyses/dla/e-76Obligated/'

pd.set_option('display.max_columns', None)



#reading data
def read_data():
    #read datasets
    # odf = pd.concat(pd.read_excel('gs://calitp-analytics-data/data-analyses/dla/e-76Obligated/obligated-projects.xlsx', sheet_name=None), ignore_index=True)
    # wdf = pd.concat(pd.read_excel('gs://calitp-analytics-data/data-analyses/dla/e-76Obligated/E-76-Waiting-list.xlsx', sheet_name=None), ignore_index=True)
    
#     #bringing the two dfs together
    # df = pd.concat([odf, wdf], ignore_index=True)
    # pd.set_option('display.max_columns', None)
    
    url = 'https://dot.ca.gov/-/media/dot-media/programs/local-assistance/documents/reports/obligated-projects.xlsx'
    df1 = pd.concat(pd.read_excel(url, sheet_name=None), ignore_index=True)

    url2 = 'https://dot.ca.gov/-/media/dot-media/programs/local-assistance/documents/reports/e-76-waiting-list.xlsx'
    df2 = pd.read_excel(url2)

    df = pd.concat([df1, df2], ignore_index=True)
    pd.set_option('display.max_columns', None)
    
    return df

#get integers without coercing other values
def get_num(x):
    try:
        return int(x)
    except Exception:
        try:
            return float(x)
        except Exception:
            return x  


#main cleaning function
def clean_data(df): 
    df = to_snakecase(df)
    df.agency = df.agency.str.title()
    
    #drop unnecessary columns
    df.drop(['waiting_days', 'unnamed:_28','today','warning'], axis=1, inplace=True)
       
    #dropping the rows with strings in 'total_requested'
    delete_row = df[df["total_requested"]== '2748.3NA999'].index
    df = df.drop(delete_row)
    
    delete_row = df[df["total_requested"]== '30169.98NA99'].index
    df = df.drop(delete_row)
    
    #change column type to float
    df['total_requested'] = df['total_requested'].astype(float)
    
    cols = ['prepared_date','to_fmis_date','submit_to_fhwa_date','submit__to_hq_date','hq_review_date','date_request_initiated','date_completed_request']
    df[cols] = df[cols].applymap(lambda x : pd.to_datetime(x, format = '%Y-%m-%d'))

    #drop duplicates
    df.drop_duplicates(inplace=True)
    df = df.dropna(how='all', axis=0)
    
    #separate out the 'project_no'
    df[["projectID", "projectNO"]] = df["project_no"].str.split(pat="(", expand=True)
    #df.projectNO = [x.replace(")", "") for x in df.projectNO]
    #drop second half
    df.drop(['projectNO'], axis=1, inplace=True)
    
    
    #get the integers of the `locode` and `projectID` column
    df['locode'] = df['locode'].apply(get_num)
    df['projectID'] = df['projectID'].apply(get_num)
    #df['fed_requested'] = df['fed_requested'].apply(get_num)
    #df['ac_requested'] = df['ac_requested'].apply(get_num)
    #df['total_requested'] = df['total_requested'].apply(get_num)
    
    #get the Prepared Year 
    df['prepared_y'] = pd.DatetimeIndex(df['prepared_date']).year
    
    #replace Non-MPO formatting
    df.mpo.replace(['NONMPO'], ['NON-MPO'], inplace=True)
    
    return df


#clean prefix codes
def prefix_cleaning(df): 
    df = read_data()
    
    df = clean_data(df)
    
    df["prefix"] = df.prefix.str.replace('-', '')
    
    replace_me = ["CASB003", "CASB05", 'CASB06','CASB07','CASB09','CASB10','CASB11','CASB12','CASB803','CASB902','CASB904','CASB905','CASB12'] 
    for i in replace_me:
        df.prefix.replace(i, 'CASB', inplace=True)
    
    replace_me = ['FERPL16', 'FERPL17','FERPL18','FERPL19','FERPLN16'] 
    for i in replace_me:
        df.prefix.replace(i, 'FERPL', inplace=True)

    replace_me = ['FSP11', 'FSP12','FSP13','FSP14'] 
    for i in replace_me:
        df.prefix.replace(i, 'FSP', inplace=True)
   
    replace_me = ['HSIP5','HISP5'] 
    for i in replace_me:
        df.prefix.replace(i, 'HSIP', inplace=True)
   
    replace_me = ['NCPD02'] 
    for i in replace_me:
        df.prefix.replace(i, 'NCPD', inplace=True)
   
    replace_me = ['PLHL04', 'PLHL05','PLHL10','PLHL11','PLHL04','PLHL05'] 
    for i in replace_me:
        df.prefix.replace(i, 'PLHL', inplace=True)

    replace_me = ['PLHDL05', 'PLHDL06','PLHDL08'] 
    for i in replace_me:
        df.prefix.replace(i, 'PLDHL', inplace=True)
    
    replace_me = ['TGR2DG.'] 
    for i in replace_me:
        df.prefix.replace(i, 'TGR2DG', inplace=True)

    replace_me = ['TCSP02', 'TCSP03','TCSP05','TCSP06','TCSP08','TCSP10'] 
    for i in replace_me:
        df.prefix.replace(i, 'TCSP', inplace=True)
    
    return df


#cleaning agency names
def clean_agency_names(df):
    
    cw1_locode =  pd.read_csv('gs://calitp-analytics-data/data-analyses/dla/e-76Obligated/agencylocode_primary_crosswalk1.csv')
    cw2_id = pd.read_csv('gs://calitp-analytics-data/data-analyses/dla/e-76Obligated/null_locodes_crosswalk2.csv')
    cw3_unmatched = pd.read_csv('gs://calitp-analytics-data/data-analyses/dla/e-76Obligated/unmatched_estimate.csv')
    
    # create a dictionary for the data
    
    locode_map1 = dict(zip(cw1_locode['primary_agency_locode'], 
                          cw1_locode['primary_agency_name']))
    
    locode_map2 = dict(zip(cw2_id['primary_agency_locode'], 
                          cw2_id['primary_agency_name']))
    
    df['primary_agency_name'] = df['locode'].map(locode_map1)
    df['primary_agency_name2'] = df['projectID'].map(locode_map2)
    
    #put converted agency names into one column, drop second column
    df['primary_agency_name'] = np.where(df.primary_agency_name.isnull(), 
                      df['primary_agency_name2'],df['primary_agency_name'])
    df.drop(['primary_agency_name2'], axis=1, inplace=True)
    
    
    #creating map
    locode_map3 = dict(zip(cw3_unmatched['agency_name'],
                           cw3_unmatched['primary_agency_name']))
    
    #filtering for null primary agency name
    df_nullagency = (df>>filter(_.primary_agency_name.isnull()))
    df_nullagency['primary_agency_name2'] = df_nullagency['agency'].map(locode_map3)
    
    #another map with df
    locode_map4 = dict(zip(df_nullagency['agency'], df_nullagency['primary_agency_name2']))
    df['primary_agency_name3'] = df['agency'].map(locode_map4)
    
    df['primary_agency_name'] = np.where(df.primary_agency_name.isnull(), 
                      df['primary_agency_name3'],df['primary_agency_name'])
    df.drop(['primary_agency_name3'], axis=1, inplace=True)
    
    return df

def adjust_prices(df):
    
    cols =  ["total_requested",
           "fed_requested",
           "ac_requested"]
    
    # Inflation table
    import cpi 
    
    def inflation_table(base_year):
        cpi.update()
        series_df = cpi.series.get(area="U.S. city average").to_dataframe()
        inflation_df = (series_df[series_df.year >= 2008]
                        .pivot_table(index='year', values='value', aggfunc='mean')
                        .reset_index()
                       )
        denominator = inflation_df.value.loc[inflation_df.year==base_year].iloc[0]

        inflation_df = inflation_df.assign(
        inflation = inflation_df.value.divide(denominator)
        )
    
        return inflation_df
#         cpi.update()
#         series_df = cpi.series.get(area="U.S. city average").to_dataframe()
#         inflation_df = (series_df[series_df.year >= 2008]
#                .pivot_table(index='year', values='value', aggfunc='mean')
#                .reset_index()
#               )
#         denominator = inflation_df.value.loc[inflation_df.year==base_year].iloc[0]

#         inflation_df = inflation_df.assign(
#             inflation = inflation_df.value.divide(denominator)
#         )
    
#         return inflation_df
    
    
    ##get cpi table 
    cpi = inflation_table(2021)
    cpi.update
    cpi = (cpi>>select(_.year, _.value))
    cpi_dict = dict(zip(cpi['year'], cpi['value']))
    
    
    for col in cols:
        multiplier = df["prepared_y"].map(cpi_dict)  
    
        ##using 270.97 for 2021 dollars
        df[f"adjusted_{col}"] = ((df[col] * 270.97) / multiplier)
    return df

#     ##get cpi table 
#     cpi_table = inflation_table(2021)
# #     cpi = (cpi>>select(_.year, _.value))
# #     cpi_dict = dict(zip(cpi['year'], cpi['value']))
    
#     df = pd.merge(df, 
#          cpi_table[["year", "multiplier"]],
#          left_on = "prepared_y",
#          right_on = "year",
#          how = "left",
#          validate = "m:1",
#         )
    
#     orig = ["total_requested", 
#         "fed_requested", 
#         "ac_requested"]

#     for c in cols:
#         df[f"adjusted_{c}"] = df.apply(lambda x: x[c] * x.multiplier, axis=1)
    
# #     for col in cols:
# #         multiplier = df["prepared_y"].map(cpi_dict)  
    
# #         ##using 270.97 for 2021 dollars
# #         df[f"adjusted_{col}"] = ((df[col] * 270.97) / multiplier)
#     return df


#add project categories

def add_categories(df):

    ACTIVE_TRANSPORTATION = ['bike', 'bicycle', 'cyclist', 
                             'pedestrian', 
                             ## including the spelling errors of `pedestrian`
                             'pedestrain',
                             'crosswalk', 
                             'bulb out', 'bulb-out', 
                             'active transp', 'traffic reduction', 
                             'speed reduction', 'ped', 'srts', 
                             'safe routes to school',
                             'sidewalk', 'side walk', 'Cl ', 'trail'
                            ]
    TRANSIT = ['bus', 'metro', 'station', #Station comes up a few times as a charging station and also as a train station
               'transit','fare', 'brt', 'yarts', 'rail'
               # , 'station' in description and 'charging station' not in description
              ] 
    BRIDGE = ["bridge", 'viaduct']
    STREET = ['traffic signal', 'resurface', 'resurfacing', 'slurry', 'seal' 
              'sign', 'stripe', 'striping', 'median', 
              'guard rail', 'guardrail', 
              'road', 'street', 
              'sinkhole', 'intersection', 'signal', 'curb',
              'light', 'tree', 'pavement', 'roundabout'
             ]

    FREEWAY = ['hov ', 'hot ', 'freeway', 'highway', 'express lanes', 'hwy']


    INFRA_RESILIENCY_ER = ['repair', 'emergency', 'replace','retrofit', 'er',
                           'rehab', 'improvements', 'seismic', 'reconstruct', 'restoration']

    CONGESTION_RELIEF = ['congestion', 'rideshare','ridesharing', 'vanpool', 'car share']

    NOT_INC = ['charging', 'fueling', 'cng']

    def categorize_project_descriptions(row):
        """
        This function takes a individual type of work description (row of a dataframe)
        and returns a dummy flag of 1 if it finds keyword present in
        project categories (active transportation, transit, bridge, etc).
        A description can contain multiple keywords across categories.
        """
        # Make lowercase
        description = row.type_of_work.lower()
    
        # Store a bunch of columns that will be flagged
        # A project can involve multiple things...also, not sure what's in the descriptions
        active_transp = 0
        transit = 0
        bridge = 0
        street = 0
        freeway = 0
        infra_resiliency_er = 0
        congestion_relief = 0
        
        if any(word in description for word in ACTIVE_TRANSPORTATION):
            active_transp = 1
        
        #if any(word in description if instanceof(word, str) else word(description) for word in TRANSIT)

        if (any(word in description for word in TRANSIT) and 
            not any(exclude_word in description for exclude_word in NOT_INC)
           ):
            transit = 1
        
        if any(word in description for word in BRIDGE):
            bridge = 1
        if any(word in description for word in STREET):
            street = 1
        if any(word in description for word in FREEWAY):
            freeway = 1 
        if any(word in description for word in INFRA_RESILIENCY_ER):
            infra_resiliency_er = 1
        if any(word in description for word in CONGESTION_RELIEF):
            congestion_relief = 1    
        
        
        return pd.Series(
            [active_transp, transit, bridge, street, freeway, infra_resiliency_er, congestion_relief], 
            index=['active_transp', 'transit', 'bridge', 'street', 
                   'freeway', 'infra_resiliency_er', 'congestion_relief']
        )
    
    
    work_categories = df.apply(categorize_project_descriptions, axis=1)
    work_cols = list(work_categories.columns)
    df2 = pd.concat([df, work_categories], axis=1)
    
    df2 = df2.assign(work_categories = df2[work_cols].sum(axis=1))
    
    return(df2)




def make_clean_data():
    df = read_data()
    df = clean_data(df)
    df = prefix_cleaning(df)
    df = clean_agency_names(df)
    df = adjust_prices(df)
    df = add_categories(df)
    
    for c in ["locode", "ftip_no", "projectID"]:
        df[c] = df[c].astype(str)
    
    return df


## running function to make_clean_data
df = make_clean_data()

## save to GCS
df.to_parquet(f"{GCS_FILE_PATH}dla_df.parquet")


## also save locally 
df.to_parquet("dla_df.parquet")

