"""
Import data from Excel spreadsheet.
Write each sheet as parquet into GCS.

Cannot use pd.read_excel() with GCS filepath.
"""
import pandas as pd
import uuid

from calitp import to_snakecase

import utils

# Use sheet_name=None to return all sheets (returns a dict)
FILENAME = "Tier_1_Facility_Location_Inventory_5-17-22.xlsx"

def read_in_sheets(FILE_PATH: str) -> dict:
    # Read in individual sheets
    # where headers are change from sheet to sheet
    df_dict = pd.read_excel(FILE_PATH, sheet_name=None)
    
    print(f"sheet names: {df_dict.keys()}")
    
    # This sheet has a bunch of footnotes
    # Essentially, we want the owned/lease tables (2) at the top, and not the other tables
    # explaining what "other" is
    office = pd.read_excel(FILE_PATH, sheet_name="Office Buildings", header=2, 
                       skipfooter = 42)
    maintenance = pd.read_excel(FILE_PATH, sheet_name = "Mtce Facilities", 
                            header=1).drop(columns = 'Unnamed: 0')
    equipment = pd.read_excel(FILE_PATH, sheet_name = "Div of Equipment Facilities")
    tmc = pd.read_excel(FILE_PATH, sheet_name = "TMCs")
    labs = pd.read_excel(FILE_PATH, sheet_name = "Labs", header = 3)
    
    # Store all the datasets in sheets as a dict
    # Will pass these through basic cleaning after
    imported_dfs = {
        "office": to_snakecase(office),
        "maintenance": to_snakecase(maintenance),
        "equipment": to_snakecase(equipment),
        "tmc": to_snakecase(tmc),
        "labs": to_snakecase(labs),
    }   
    
    return imported_dfs

# Cursory cleaning of column names applicable to all sheets
def clean_column_names(df: pd.DataFrame) -> pd.DataFrame:
    # Get rid of those asterisks, double underscores, and leading/trailing underscores
    df.columns = (df.columns.str.replace("*", "", regex=True)
                  .str.replace(r'_{2}', '', regex=True)
                  .str.strip('_')
                 )
    
    # Rename zip to zip_code
    df = df.rename(columns = {"zip": "zip_code"})

    return df


def generate_uuid(df: pd.DataFrame) -> pd.DataFrame:
    '''
    Need a uuid to be able to reference individual sheets.
    Later, compile it into 1 df (but there might be duplicates by addresses, and
    those duplicates would have different uuids)
    '''
    df["sheet_uuid"] = df.apply(lambda _: str(uuid.uuid4()), axis=1)
    return df
    
    
def clean_office(df: pd.DataFrame) -> pd.DataFrame:
    """
    Basic cleaning of the `office` sheet in Excel, 
    and only keep columns that store row-level info.
    
    Any columns that are aggregations get dropped, 
    because keeping them will give us redundant info.
    """
    df = clean_column_names(df)
    
    rename_cols = {
        "unnamed:_2": "district_name",
        "ownedoleasedl": "owned_or_leased",    
    }
    
    drop_cols = [
        'district_total_gross_spaceowned_gross_leased',
        'district_total_net_spaceowned_net_leased']
    
    # Drop the lines where comments are inserted
    # Also drop where District / Geographic District / Grand Totals are given
    df2 =  df.dropna(subset="address")
    df3 = df2[~((df2.address.str.contains("Total")) |
              (df2.address.str.contains("Address")))
             ].reset_index(drop=True)

    df3 = (df3.assign(
              district = df3.district.ffill().str.replace(' ', '')
          ).rename(columns = rename_cols)
           .drop(columns = drop_cols)
         )
    
    df3 = generate_uuid(df3)
    
    return df3

def clean_maintenance(df: pd.DataFrame) -> pd.DataFrame:
    """
    Basic cleaning of the `maintenance` sheet in Excel.
    
    Include the legend on the side of Excel sheet and 
    populate the column with it.
    """
    df = clean_column_names(df)
    drop_cols = ['unnamed:_9', 'unnamed:_10', 'legend']
    
    rename_cols = {
        "dist": "district",
    }
    
    df = (df.assign(
        zip_code = df.zip_code.astype(str),
        mtce_facility_name = df.mtce_facility_name.str.title(),
        city = df.city.str.title(),
        facility_type = df.apply(lambda x: "Maintenance Station" if x.ms_ss == "MS"
                                 else "Stand-alone Sand Salt Storage Sheds", axis=1)
        ).drop(columns = drop_cols)
        .rename(columns = rename_cols)
    )
    
    df = generate_uuid(df)

    return df

def clean_equipment(df: pd.DataFrame) -> pd.DataFrame:
    """
    Basic cleaning of the `equipment` sheet in Excel.
    """
    df = clean_column_names(df)
    df = generate_uuid(df)
    
    return df

def clean_tmc(df: pd.DataFrame) -> pd.DataFrame:
    """
    Basic cleaning of the `tmc` sheet in Excel.
    """
    df = clean_column_names(df)
    df = generate_uuid(df)
    
    return df

def clean_labs(df: pd.DataFrame) -> pd.DataFrame:
    """
    Basic cleaning of the `labs` sheet in Excel.
    """
    df = clean_column_names(df)
    df = df.assign(
        zip_code = df.zip_code.astype("Int64")
    )
    
    df = generate_uuid(df)

    return df

    
if __name__ == "__main__":
    # Read in sheets correctly and save as dict
    imported_dfs = read_in_sheets(f"{utils.DATA_PATH}{FILENAME}")
    
    # Map the cleaning function to each dataset
    cleaning_functions = {
        "office": clean_office,   
        "maintenance": clean_maintenance,
        "equipment": clean_equipment,
        "tmc": clean_tmc,
        "labs": clean_labs,
    }
    
    # Do basic cleaning, write to parquet
    cleaned_dfs = {}
    for key, data in imported_dfs.items():
        # key: dataset_name; value: df
        # Apply the cleaning function for each specific dataset 
        # by keying into the dataset's name
        cleaned_dfs[key] = cleaning_functions[key](data)
        
    for key, value in cleaned_dfs.items():
        value.to_parquet(f"{utils.GCS_FILE_PATH}{key}.parquet")
        
    print("Finished saving individual parquets")