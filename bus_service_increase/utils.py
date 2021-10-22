"""
Utility functions
"""
import gcsfs
import pandas as pd

SQ_MI_PER_SQ_M = 3.86 * 10**-7
GCS_PROJECT = "cal-itp-data-infra"
BUCKET_NAME = "calitp-analytics-data"
BUCKET_DIR = "bus_service_increase"
GCS_FILE_PATH = f"gs://{BUCKET_NAME}/{BUCKET_DIR}/"

def import_export(DATASET_NAME, OUTPUT_FILE_NAME, GCS=True): 
    """
    DATASET_NAME: str. Name of csv dataset.
    OUTPUT_FILE_NAME: str. Name of output parquet dataset.
    """
    df = pd.read_csv(f"{DATASET_NAME}.csv")    
    
    if GCS is True:
        df.to_parquet(f"{GCS_FILE_PATH}{OUTPUT_FILE_NAME}.parquet")
    else:
        df.to_parquet(f"./{OUTPUT_FILE_NAME}.parquet")
    
        

def define_equity_groups(df, percentile_col = ["CIscoreP"], num_groups=5):
    """
    df: pandas.DataFrame
    percentile_col: list.
                    List of columns with values that are percentils, to be
                    grouped into bins.
    num_groups: integer.
                Number of bins, groups. Ex: for quartiles, num_groups=4.
    """
    
    for col in percentile_col:
        df = df.assign(group_col = 0)

        bin_range = round(100 / num_groups)

        for i in range(1, num_groups + 1):
            max_cutoff = i * bin_range
            df = df.assign(
                group_col = df.apply(
                    lambda x: i if (x[col] <= max_cutoff) and 
                    (x[col] >= max_cutoff - 19)
                    else x.group_col, axis = 1),
            )
        df = df.rename(columns = {"group_col": f"{col}_group"})
    
    return df


def prep_calenviroscreen(df):
    # Fix tract ID and calculate pop density
    df = df.assign(
        Tract = df.Tract.apply(lambda x: '0' + str(x)[:-2]).astype(str),
        sq_mi = df.geometry.area * SQ_MI_PER_SQ_M,
    )
    df['pop_sq_mi'] = df.Population / df.sq_mi
    
    df2 = define_equity_groups(
        df,
        percentile_col =  ["CIscoreP", "Pollution_", "PopCharP"], 
        num_groups = 5 )
    
    # Rename columns
    keep_cols = [
        'Tract', 'ZIP', 'Population',
        'sq_mi', 'pop_sq_mi',
        'CIscoreP', 'Pollution_', 'PopCharP',
        'CIscoreP_group', 'Pollution__group', 'PopCharP_group',
        'County', 'City_1', 'geometry',  
    ]
    
    df3 = (df2[keep_cols]
           .rename(columns = 
                     {"CIscoreP_group": "equity_group",
                     "Pollution__group": "pollution_group",
                     "PopCharP_group": "popchar_group",
                     "City_1": "City",
                     "CIscoreP": "overall_ptile",
                     "Pollution_": "pollution_ptile",
                     "PopCharP": "popchar_ptile"}
                    )
           .sort_values(by="Tract")
           .reset_index(drop=True)
          )
    
    return df3
