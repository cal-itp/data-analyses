"""
Generate CalEnviroScreen and LEHD data.

CalEnviroScreen 4.0:
https://oehha.ca.gov/calenviroscreen/report/calenviroscreen-40

LEHD Data:
https://datacatalog.urban.org/dataset/longitudinal-employer-household-dynamics-origin-destination-employment-statistics-lodes

LEHD Data Dictionary:
https://datacatalog.urban.org/sites/default/files/data-dictionary-files/LODESTechDoc7.5.pdf
"""
import geopandas as gpd
import intake
import numpy as np
import pandas as pd

import shared_utils
import utils

catalog = intake.open_catalog("./catalog.yml")

#--------------------------------------------------------#
### CalEnviroScreen functions
#--------------------------------------------------------#
def define_equity_groups(df: pd.DataFrame, 
                         percentile_col: list = ["CIscoreP"], 
                         num_groups: int=5) -> pd.DataFrame:
    """
    df: pandas.DataFrame
    percentile_col: list.
                    List of columns with values that are percentils, to be
                    grouped into bins.
    num_groups: integer.
                Number of bins, groups. Ex: for quartiles, num_groups=4.
                
    `pd.cut` vs `pd.qcut`: 
    https://stackoverflow.com/questions/30211923/what-is-the-difference-between-pandas-qcut-and-pandas-cut            
    """
    
    for col in percentile_col:
        # -999 should be replaced as NaN, so it doesn't throw off the binning of groups
        df = (df.assign(
                col = df[col].replace(-999, np.nan),
                new_col = pd.cut(df[col], bins=num_groups, labels=False) + 1
            ).drop(columns = col) # drop original column and use the one with replaced values
            .rename(columns = {
                "col": col, 
                "new_col": f"{col}_group"})
             )

    return df


def prep_calenviroscreen(df: pd.DataFrame)-> pd.DataFrame:
    # Fix tract ID and calculate pop density
    df = (df.assign(
            Tract = df.Tract.apply(lambda x: '0' + str(x)[:-2]).astype(str),
            sq_mi = df.geometry.area * shared_utils.geography_utils.SQ_MI_PER_SQ_M,
        ).rename(columns = {
            "TotPop19": "Population",
            "ApproxLoc": "City",
        })
    )
    df['pop_sq_mi'] = df.Population / df.sq_mi
    
    df2 = define_equity_groups(
        df,
        percentile_col =  ["CIscoreP", "PolBurdP", "PopCharP"], 
        num_groups = 3)
    
    # Rename columns
    keep_cols = [
        'Tract', 'ZIP', 'Population',
        'sq_mi', 'pop_sq_mi',
        'CIscoreP', 'PolBurdP', 'PopCharP',
        'CIscoreP_group', 'PolBurdP_group', 'PopCharP_group',
        'County', 'City', 'geometry',  
    ]
    
    df3 = (df2[keep_cols]
           .rename(columns = 
                     {"CIscoreP_group": "equity_group",
                     "PolBurdP_group": "pollution_group",
                     "PopCharP_group": "popchar_group",
                     "CIscoreP": "overall_ptile",
                     "PolBurdP": "pollution_ptile",
                     "PopCharP": "popchar_ptile"}
                    )
           .sort_values(by="Tract")
           .reset_index(drop=True)
          )
    
    return df3



#--------------------------------------------------------#
### LEHD functions
#--------------------------------------------------------#
# Download LEHD data from Urban Institute
URBAN_URL = "https://urban-data-catalog.s3.amazonaws.com/drupal-root-live/"
DATE_DOWNLOAD = "2021/04/19/"

# Doing all_se01-se03 is the same as primary jobs
# summing up to tract level gives same df.describe() results
LEHD_DATASETS = ["wac_all_se01_tract_minus_fed", 
            "wac_all_se02_tract_minus_fed",
            "wac_all_se03_tract_minus_fed", 
            "wac_fed_tract"]

'''
for dataset in LEHD_DATASETS:
    shared_utils.utils.import_csv_export_parquet(
        DATASET_NAME = f"{URBAN_URL}{DATE_DOWNLOAD}{dataset}",
        OUTPUT_FILE_NAME = dataset, 
        GCS_FILE_PATH = utils.GCS_FILE_PATH,
        GCS=True
    )
'''

def process_lehd(df: pd.DataFrame) -> pd.DataFrame:
    # Subset to CA, keep maxiumum year, and only keep total jobs
    keep_cols = ["trct", "c000"]
    
    df = (df[(df.stname == "California") & 
            (df.year == df.year.max())]
          [keep_cols]
          .assign(
              trct = df.apply(lambda x: '0' + str(x.trct), axis = 1).astype(str),
          )
          .rename(columns = {"trct": "Tract", 
                            "c000": "num_jobs"})
          .reset_index(drop=True)          
    )
    
    return df


#--------------------------------------------------------#
### Functions to merge CalEnviroScreen and LEHD 
#--------------------------------------------------------#
# Merge and clean up 
def merge_and_process(data_to_merge: list = []) -> pd.DataFrame:
    """
    data_to_merge: list. List of pandas.DataFrames to merge.
    """    
    # Use a loop to merge dataframes
    # For 1st df, make a copy and rename column
    # For subsequent dfs, merge and rename column
    # We want num_jobs to be stored as num_jobs0, num_jobs1, etc to keep track
    final = pd.DataFrame()
    i = 0
    
    for d in data_to_merge:
        new_col = f"num_jobs{i}"
        if i == 0:
            final = d.copy()
            final = final.rename(columns = {"num_jobs": new_col})
            
        else:
            final = final.merge(d,
                on = "Tract", how = "left", validate = "1:1"
            ).rename(columns = {"num_jobs": new_col})
            
        i += 1
    
    # Sum across to get total number of jobs for tract
    jobs_cols = [col for col in final.columns if "num_jobs" in col]
    
    final = final.assign(
        num_jobs = final[jobs_cols].sum(axis=1).astype(int),
    ).drop(columns = jobs_cols)
        
    return final   
    
    
def merge_calenviroscreen_lehd(calenviroscreen: pd.DataFrame, 
                               lehd: pd.DataFrame) -> pd.DataFrame:    
    # Merge LEHD with CalEnviroScreen
    df = pd.merge(calenviroscreen, lehd, 
                  on = "Tract", how = "left", validate = "1:1"
                 )
    
    # Calculate jobs per sq mi
    df = df.assign(
        num_jobs = df.num_jobs.fillna(0).astype(int),
        jobs_sq_mi = df.num_jobs / df.sq_mi,
    )
    
    # Add a pop + jobs per sq mi
    df = df.assign(
        num_pop_jobs = df.Population + df.num_jobs,
        popjobs_sq_mi = (df.Population + df.num_jobs) / df.sq_mi
    )
    
    return df


## Put all functions above together to generate cleaned CalEnviroScreen + LEHD data
def generate_calenviroscreen_lehd_data() -> pd.DataFrame:
    # CalEnviroScreen data (gdf)
    gdf = catalog.calenviroscreen_raw.read()
    gdf = prep_calenviroscreen(gdf)
    
    # LEHD Data
    lehd_dfs = {}
    for d in LEHD_DATASETS:
        lehd_dfs[d] = pd.read_parquet(f"{utils.GCS_FILE_PATH}{d}.parquet")
    
    cleaned_dfs = []
    for key, value in lehd_dfs.items():
        processed_df = process_lehd(value)
        cleaned_dfs.append(processed_df)

    lehd = merge_and_process(data_to_merge = cleaned_dfs)
    
    # Merge together
    df = merge_calenviroscreen_lehd(gdf, lehd)
    
    return df





