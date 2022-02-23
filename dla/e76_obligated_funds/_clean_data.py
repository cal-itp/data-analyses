"""
Cleaning DLA Data

Obligated List:
https://dot.ca.gov/programs/local-assistance/reports/e-76-obligated

Waiting List: 
https://dot.ca.gov/programs/local-assistance/reports/e-76-waiting


"""



import numpy as np
import pandas as pd
from calitp import to_snakecase
from siuba import *
#import cpi

pd.set_option('display.max_columns', None)

#reading data
def read_data():
    #read datasets
#     odf = pd.concat(pd.read_excel('gs://calitp-analytics-data/data-analyses/dla/e-76Obligated/obligated-projects.xlsx', sheet_name=None), ignore_index=True)
#     wdf = pd.concat(pd.read_excel('gs://calitp-analytics-data/data-analyses/dla/e-76Obligated/E-76-Waiting-list.xlsx', sheet_name=None), ignore_index=True)
    
#     #bringing the two dfs together
#     df = pd.concat([odf, wdf], ignore_index=True)
#     pd.set_option('display.max_columns', None)
    
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
    
    ##get cpi table 
    cpi = inflation_table(2021)
    cpi = (cpi>>select(_.year, _.value))
    cpi_dict = dict(zip(cpi['year'], cpi['value']))
    
    
    for col in cols:
        multiplier = df["prepared_y"].map(cpi_dict)  
    
        ##using 270.97 for 2021 dollars
        df[f"adjusted_{col}"] = ((df[col] * 270.97) / multiplier)
    return df



def make_clean_data():
    df = read_data()
    df = clean_data(df)
    df = prefix_cleaning(df)
    df = clean_agency_names(df)
    df = adjust_prices(df)
    
    for c in ["locode", "ftip_no", "projectID"]:
        df[c] = df[c].astype(str)
    
    return df





