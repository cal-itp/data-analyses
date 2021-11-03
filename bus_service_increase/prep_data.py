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
import pandas as pd

import utils
import calenviroscreen_utils

# Download LEHD data from Urban Institute
URBAN_URL = "https://urban-data-catalog.s3.amazonaws.com/drupal-root-live/"
DATE_DOWNLOAD = "2021/04/19/"

# Doing all_se01-se03 is the same as primary jobs
# summing up to tract level gives same df.describe() results
datasets = ["wac_all_se01_tract_minus_fed", 
            "wac_all_se02_tract_minus_fed",
            "wac_all_se03_tract_minus_fed", 
            "wac_fed_tract"]

for dataset in datasets:
    utils.import_export(DATASET_NAME = f"{URBAN_URL}{DATE_DOWNLOAD}{dataset}", 
                        OUTPUT_FILE_NAME = dataset, GCS=True)

#--------------------------------------------------------#
### LEHD functions
#--------------------------------------------------------#
def process_lehd(df):
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


# Merge and clean up 
def merge_and_process(data_to_merge = []):
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
    
    
def merge_calenviroscreen_lehd(calenviroscreen, lehd):
    gdf = calenviroscreen_utils.prep_calenviroscreen(calenviroscreen)
    
    # Merge LEHD with CalEnviroScreen
    df = pd.merge(gdf, lehd, 
                  on = "Tract", how = "left", validate = "1:1"
                 )
    
    # Calculate jobs per sq mi
    df = df.assign(
        num_jobs = df.num_jobs.fillna(0).astype(int),
        jobs_sq_mi = df.num_jobs / df.sq_mi,
    )
    
    return df


#--------------------------------------------------------#
### Function to make cleaned data
#--------------------------------------------------------#
def generate_calenviroscreen_lehd_data(lehd_datasets):
    # CalEnviroScreen data (gdf)
    CALENVIROSCREEN_FILE = 'calenviroscreen40shp_F_2021/CES4_final.shp'
    gdf = gpd.read_file(f"./{CALENVIROSCREEN_FILE}")
    
    # LEHD Data
    lehd_dfs = {}
    for d in lehd_datasets:
        lehd_dfs[d] = pd.read_parquet(f"{utils.GCS_FILE_PATH}{d}.parquet")
    
    cleaned_dfs = []
    for key, value in lehd_dfs.items():
        processed_df = process_lehd(value)
        cleaned_dfs.append(processed_df)

    lehd = merge_and_process(data_to_merge = cleaned_dfs)
    
    # Merge together
    df = merge_calenviroscreen_lehd(gdf, lehd)
    
    return df


# Stop times by tract
def generate_stop_times_tract_data():
    df = gpd.read_parquet("./bus_stop_times_by_tract.parquet")

    df = df.assign(
        num_arrivals = df.num_arrivals.fillna(0),
        popdensity_group = pd.qcut(df.pop_sq_mi, q=3, labels=False) + 1,
        jobdensity_group = pd.qcut(df.jobs_sq_mi, q=3, labels=False) + 1,
    )

    df = df.assign(
        avg_density_score = round(
            df[["jobdensity_group", "popdensity_group"]].sum(axis=1) / 2, 
            0),
        arrivals_sq_mi = df.num_arrivals / df.sq_mi,
        arrivals_per_1k = (df.num_arrivals / df.Population) * 1_000,
    )
    
    df = df.assign(
        arrivals_group = pd.qcut(df.arrivals_per_1k, q=3, labels=False) + 1,
    )

    return df