"""
Generate CalEnviroScreen and LEHD data.

CalEnviroScreen 4.0:
https://oehha.ca.gov/calenviroscreen/report/calenviroscreen-40

LEHD Data:
https://datacatalog.urban.org/dataset/longitudinal-employer-household-dynamics-origin-destination-employment-statistics-lodes

LEHD Data Dictionary:
https://datacatalog.urban.org/sites/default/files/data-dictionary-files/LODESTechDoc7.5.pdf
"""
import fsspec
import geopandas as gpd
import numpy as np
import pandas as pd

from calitp_data_analysis import geography_utils, utils
from bus_service_utils import utils as bus_utils

#--------------------------------------------------------#
### CalEnviroScreen functions
#--------------------------------------------------------#
def define_equity_groups(df: pd.DataFrame, 
                         percentile_col: list, 
                         num_groups: int) -> pd.DataFrame:
    """
    df: pandas.DataFrame
    percentile_col: list.
                    List of columns with values that are percentiles, to be
                    grouped into bins.
    num_groups: integer.
                Number of bins, groups. Ex: for quartiles, num_groups=4.
                
    `pd.cut` vs `pd.qcut`: 
    https://stackoverflow.com/questions/30211923/what-is-the-difference-between-pandas-qcut-and-pandas-cut            
    """
    for col in percentile_col:
        df[f"{col}_group"] = (pd.qcut(
            df[col], num_groups, labels=False) + 1).astype("Int64")
            

    return df


def prep_calenviroscreen(df: gpd.GeoDataFrame, 
                         quartile_groups: int = 3,
                        )-> gpd.GeoDataFrame:
    # Fix tract ID and calculate pop density
    df = (df.assign(
            Tract = df.Tract.apply(lambda x: '0' + str(x)[:-2]).astype(str),
            sq_mi = df.geometry.area * geography_utils.SQ_MI_PER_SQ_M,
        ).rename(columns = {
            "TotPop19": "Population",
            "ApproxLoc": "City",
        })
    )
    df['pop_sq_mi'] = df.Population / df.sq_mi
    
    
    # Rename columns
    keep_cols = [
        'Tract', 'ZIP', 'Population',
        'sq_mi', 'pop_sq_mi',
        'CIscoreP', 'PolBurdP', 'PopCharP',
        'County', 'City', 'geometry',  
    ]
    
    df2 = (df[keep_cols]
           .rename(columns = {
                     "CIscoreP": "overall_ptile",
                     "PolBurdP": "pollution_ptile",
                     "PopCharP": "popchar_ptile"})
           .sort_values(by="Tract")
           .reset_index(drop=True)
          )
    
    df3 = define_equity_groups(
        df2,
        percentile_col =  ["overall_ptile"], 
        num_groups = quartile_groups
    ).rename(columns = {"overall_ptile_group": "equity_group"})
    
    return df3



#--------------------------------------------------------#
### LEHD functions
#--------------------------------------------------------#
LEHD_DATASETS = [
    "wac_all_se01_tract_minus_fed", 
    "wac_all_se02_tract_minus_fed",
    "wac_all_se03_tract_minus_fed", 
    "wac_fed_tract"
]

def download_lehd_data(download_date: str, 
                       lehd_datasets: list = LEHD_DATASETS):
    """
    Download LEHD data from Urban Institute
    """
    URBAN_URL = "https://urban-data-catalog.s3.amazonaws.com/drupal-root-live/"

    # Doing all_se01-se03 is the same as primary jobs
    # summing up to tract level gives same df.describe() results

    for dataset in lehd_datasets:
        dataset_name = f"{URBAN_URL}{download_date}{dataset}.csv"
        df = pd.read_csv(datdaset_name)
        df.to_parquet(f"{bus_utils.GCS_FILE_PATH}{dataset}.parquet")


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
    
    for i, d in enumerate(data_to_merge):
        if i == 0:
            final = d.copy()
            final = final.rename(columns = {"num_jobs": f"num_jobs{i}"})
            
        else:
            final = final.merge(d,
                on = "Tract", how = "left", validate = "1:1"
            ).rename(columns = {"num_jobs": f"num_jobs{i}"})
                
    # Sum across to get total number of jobs for tract
    jobs_cols = [col for col in final.columns if "num_jobs" in col]
    
    final = final.assign(
        num_jobs = final[jobs_cols].sum(axis=1).astype(int),
    ).drop(columns = jobs_cols)
        
    return final   
    
    
def merge_calenviroscreen_lehd(
    calenviroscreen: pd.DataFrame, 
    lehd: pd.DataFrame
) -> pd.DataFrame:    
    # Merge LEHD with CalEnviroScreen
    df = pd.merge(
        calenviroscreen, 
        lehd, 
        on = "Tract", 
        how = "left", 
        validate = "1:1"
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
def generate_calenviroscreen_lehd_data(
    calenviroscreen_quartile_groups: int,
    lehd_datasets: list,
    GCS: bool
) -> gpd.GeoDataFrame:
    
    # CalEnviroScreen data (gdf)
    CALENVIROSCREEN_PATH = f"{bus_utils.GCS_FILE_PATH}calenviroscreen40shpf2021shp.zip"
    
    with fsspec.open(CALENVIROSCREEN_PATH) as file:
        gdf = gpd.read_file(file)
    
    gdf = prep_calenviroscreen(gdf, calenviroscreen_quartile_groups)
    
    # LEHD Data
    cleaned_dfs = [
        pd.read_parquet(
            f"{bus_utils.GCS_FILE_PATH}{d}.parquet"
        ).pipe(process_lehd) for d in lehd_datasets
    ]

    lehd = merge_and_process(cleaned_dfs)
    
    # Merge together
    final = merge_calenviroscreen_lehd(gdf, lehd)
    
    if GCS:
        utils.geoparquet_gcs_export(
            final,
            bus_utils.GCS_FILE_PATH,
            "calenviroscreen_lehd_by_tract"
        )
    
    return final
