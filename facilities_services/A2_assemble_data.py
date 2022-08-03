"""
Read in raw parquet, standardize across datasets
to create 1 dataframe that can be geocoded.

Look across the the dataframes, find what's in common, and 
do a more detailed data processing before assembling into 1 combined dataset.
"""
import numpy as np
import pandas as pd

import utils

# Set up cleaning functions to standardize 
# across datasets
# Keep address info, sheet_uuid, standardize column naming and types
# keep sqft info, district, county info if available
def clean_office(df: pd.DataFrame) -> pd.DataFrame:
    keep_cols = [
        "district", "address", "district_name", "owned_or_leased",
        "owned_net", "leased", "other",
        "sheet_uuid",
    ]

    # standardize how district is across other sheets
    df = (df[keep_cols]
          .assign(
              district = df.apply(lambda x: "59" if x.district=="HQ"
                                  else x.district.replace("D", ""), axis=1).astype(int),
              sqft = df[["owned_net", "leased", "other"]].sum(axis=1),
              category = "office",
          ).drop(columns = ["owned_net", "leased"])
        # It looks like district_name is related to the city of the address
        # Diamond Bar is listed as part of District 8. 
        # DB is the city of the address, not the caltrans district name.
          .rename(columns = {"district_name": "city"})
    )
    
    return df

def clean_maintenance(df: pd.DataFrame) -> pd.DataFrame:
    rename_cols = {
        "mtce_facility_name": "facility_name",
    }
        
    keep_cols = [
        "district", "location_code", "facility_name", 
        "address", "city", "county", "zip_code", 
        "facility_type",
        "sheet_uuid"
    ]
    
    df = (df.rename(columns = rename_cols)
          [keep_cols]
          .assign(
              category = "maintenance",    
              zip_code = df.zip_code.replace("nan", np.nan)
          )
         )
    
    # Further clean up zip_code
    # Some addresses don't look descriptive, keep the 4 digit second part of zip code
    df = df.assign(
        zip_code2 = df.zip_code.str.split("-", expand=True)[1].astype(str),
        zip_code = df.zip_code.str.split("-", expand=True)[0].astype("Int64"),
    )
    
    return df

def clean_equipment(df: pd.DataFrame) -> pd.DataFrame:
    rename_cols = {
        "facility": "facility_name",
        "street": "address",
    }
    
    keep_cols = [
        "facility_name", 
         "address", "city", "zip_code",
         "sheet_uuid"
    ]
    
    df = (df.rename(columns = rename_cols)
          [keep_cols]
          .assign(
              category = "equipment"
          )
    )
    
    return df

def clean_labs(df: pd.DataFrame) -> pd.DataFrame:
    rename_cols = {
        "classification_nametype_of_facility": "facility_type",
        "asset_name": "facility_name",
        "sq_ft": "sqft",
        "proposed_category": "lab_category",
    }
    
    keep_cols = [
        'facility_type', 'district',
        'asset_id', 'sqft', 'facility_name', 'address', 'city', 'zip_code',
        'lab_category', 'sheet_uuid'
    ]
    
    df = (df.rename(columns = rename_cols)
          [keep_cols]
          .assign(
              category = "labs",
              owned_or_leased = df.apply(lambda x: "O" if "own" in x.purchase_type.lower()
                                         else "L", axis=1)
          )
    )
    return df


if __name__ == "__main__":

    datasets = [
        "office", "maintenance", "equipment", "labs"
    ]
    
    # Map another cleaning function to each dataset
    more_cleaning_dict = {
        "office": clean_office,
        "maintenance": clean_maintenance,
        "equipment": clean_equipment,
        # TMCs sheet doesn't have any address info to be geocoded
        "labs": clean_labs,
    }
    
    # Store the processing in a dict, in case we need to double check and add more cleaning
    processed_dfs = {}
    for d in datasets:
        df1 = pd.read_parquet(f"{utils.GCS_FILE_PATH}{d}.parquet")
        processed_dfs[d] = more_cleaning_dict[d](df1)

    # Append these individual datasets together    
    df = pd.DataFrame()
    for key, value in processed_dfs.items():
        df = pd.concat([df, value], axis=0, ignore_index=True)
    
    # Export to GCS
    df.to_parquet(f"{utils.GCS_FILE_PATH}tier1_facilities_for_geocoding.parquet")
    print("Assembled 1 df together")