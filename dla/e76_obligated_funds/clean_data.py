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



#change format of columns to integer
def get_num(x):
    try:
        return int(x)
    except Exception:
        try:
            return float(x)
        except Exception:
            return x       
        
        
#reading data
def read_data(): 
    #read datasets
    odf = pd.concat(pd.read_excel('gs://calitp-analytics-data/data-analyses/dla/e-76Obligated/obligated-projects.xlsx', sheet_name=None), ignore_index=True)
    wdf = pd.concat(pd.read_excel('gs://calitp-analytics-data/data-analyses/dla/e-76Obligated/E-76-Waiting-list.xlsx', sheet_name=None), ignore_index=True)
    
    #bringing the two dfs together
    df = pd.concat([odf, wdf], ignore_index=True)
    return df


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
    df.projectNO = [x.replace(")", "") for x in df.projectNO]
    #drop second half
    df.drop(['projectNO'], axis=1, inplace=True)
    
    def get_num(x):
        try:
            return int(x)
        except Exception:
            try:
                return float(x)
            except Exception:
                return x  
    
    #get the integers of the `locode` and `projectID` column
    df['locode'] = df['locode'].apply(get_num)
    df['projectID'] = df['projectID'].apply(get_num)
    
    #replace Non-MPO formatting
    df.mpo.replace(['NONMPO'], ['NON-MPO'], inplace=True)
    
    return df


#clean prefix codes
def prefix_cleaning(df): 
    df = read_data()
    
    df = clean_data(df)
    
    df.prefix.replace(['BR-'], ['BR'], inplace=True)

    df.prefix.replace(['ATP-CML'], ['ATPCML'], inplace=True)

    df.prefix.replace(['CASB003'], ['CASB'], inplace=True)
    df.prefix.replace(['CASB05'], ['CASB'], inplace=True)
    df.prefix.replace(['CASB06'], ['CASB'], inplace=True)
    df.prefix.replace(['CASB07'], ['CASB'], inplace=True)
    df.prefix.replace(['CASB09'], ['CASB'], inplace=True)
    df.prefix.replace(['CASB10'], ['CASB'], inplace=True)
    df.prefix.replace(['CASB11'], ['CASB'], inplace=True)
    df.prefix.replace(['CASB12'], ['CASB'], inplace=True)
    df.prefix.replace(['CASB803'], ['CASB'], inplace=True)
    df.prefix.replace(['CASB902'], ['CASB'], inplace=True)
    df.prefix.replace(['CASB904'], ['CASB'], inplace=True)
    df.prefix.replace(['CASB905'], ['CASB'], inplace=True)
    df.prefix.replace(['CASB12'], ['CASB'], inplace=True)

    df.prefix.replace(['FERPL16'], ['FERPL'], inplace=True)
    df.prefix.replace(['FERPL17'], ['FERPL'], inplace=True)
    df.prefix.replace(['FERPL18'], ['FERPL'], inplace=True)
    df.prefix.replace(['FERPL19'], ['FERPL'], inplace=True)
    df.prefix.replace(['FERPLN16'], ['FERPL'], inplace=True)

    df.prefix.replace(['FSP11'], ['FSP'], inplace=True)
    df.prefix.replace(['FSP12'], ['FSP'], inplace=True)
    df.prefix.replace(['FSP13'], ['FSP'], inplace=True)
    df.prefix.replace(['FSP14'], ['FSP'], inplace=True)

    df.prefix.replace(['RPSTPLE-'], ['RPSTPLE'], inplace=True)

    df.prefix.replace(['RPSTPLE-'], ['RPSTPLE'], inplace=True)

    df.prefix.replace(['HSIP5'], ['HSIP'], inplace=True)
    df.prefix.replace(['HSIIPL'], ['HSIPL'], inplace=True)
    df.prefix.replace(['HISP5'], ['HSIP'], inplace=True)


    df.prefix.replace(['NCPD02'], ['NCPD'], inplace=True)


    df.prefix.replace(['PLHL04'], ['PLHL'], inplace=True)
    df.prefix.replace(['PLHL05'], ['PLHL'], inplace=True)
    df.prefix.replace(['PLHL10'], ['PLHL'], inplace=True)
    df.prefix.replace(['PLHL11'], ['PLHL'], inplace=True)

    df.prefix.replace(['PLHDL05'], ['PLDHL'], inplace=True)
    df.prefix.replace(['PLHDL06'], ['PLDHL'], inplace=True)
    df.prefix.replace(['PLHDL08'], ['PLDHL'], inplace=True)
    df.prefix.replace(['PLHL04'], ['PLHL'], inplace=True)
    df.prefix.replace(['PLHL05'], ['PLHL'], inplace=True)

    df.prefix.replace(['RPSTPL-'], ['RPSTPL'], inplace=True)

    df.prefix.replace(['SRTSL-'], ['SRTSL'], inplace=True)

    df.prefix.replace(['TGR2DG.'], ['TGR2DG'], inplace=True)

    df.prefix.replace(['TCSP02'], ['TCSP'], inplace=True)
    df.prefix.replace(['TCSP03'], ['TCSP'], inplace=True)
    df.prefix.replace(['TCSP05'], ['TCSP'], inplace=True)
    df.prefix.replace(['TCSP06'], ['TCSP'], inplace=True)
    df.prefix.replace(['TCSP08'], ['TCSP'], inplace=True)
    df.prefix.replace(['TCSP10'], ['TCSP'], inplace=True)
    
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







