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

# Download LEHD data from Urban Institute
URBAN_URL = "https://urban-data-catalog.s3.amazonaws.com/drupal-root-live/"
DATE_DOWNLOAD = "2021/04/19/"
datasets = ["wac_pri_tract_minus_fed", "wac_fed_tract"]

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
def merge_and_process(df1, df2):
    df = pd.merge(df1, 
              df2.rename(columns = {"num_jobs": "fed_jobs"}), 
              on = "Tract", how = "left", validate = "1:1")
    
    df = df.assign(
        wac_num_jobs = df[["num_jobs", "fed_jobs"]].sum(axis=1).astype(int)
    )[["Tract", "wac_num_jobs"]]
    
    return df    
    
    
def merge_calenviroscreen_lehd(calenviroscreen, lehd):
    gdf = utils.prep_calenviroscreen(calenviroscreen)
    
    # Merge LEHD with CalEnviroScreen
    df = pd.merge(gdf, lehd, 
                  on = "Tract", how = "left", validate = "1:1")
    
    df = df.assign(
        wac_num_jobs = df.wac_num_jobs.fillna(0).astype(int)
    )
    
    return df


#--------------------------------------------------------#
### Function to make cleaned data
#--------------------------------------------------------#
def generate_calenviroscreen_lehd_data():
    # CalEnviroScreen data (gdf)
    CALENVIROSCREEN_FILE = 'calenviroscreen40shp_F_2021/CES4_final.shp'
    gdf = gpd.read_file(f"./{CALENVIROSCREEN_FILE}")
    
    # LEHD Data
    primary_nofed = pd.read_parquet((f"gs://{utils.BUCKET_NAME}/"
                                     "wac_pri_tract_minus_fed.parquet")
                                   )
    fed = pd.read_parquet(f"gs://{utils.BUCKET_NAME}/"
                          "wac_fed_tract.parquet")

    primary_nofed = process_lehd(primary_nofed)
    fed = process_lehd(fed)

    lehd = merge_and_process(primary_nofed, fed)
    
    # Merge together
    df = merge_calenviroscreen_lehd(gdf, lehd)
    
    return df