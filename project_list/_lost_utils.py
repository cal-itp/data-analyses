import camelot
import pandas as pd

def clean_LOST_pdf(df, row_to_keep:int, 
              file_name:str,
              notes: str):
    """
    Returns a cleaner dataframe.
    row_to_keep (int): the beginning of the dataframe.
    EX: 6, 13, 20. 
    """
    # Get rid of the unnecessary header info
    df = df.iloc[row_to_keep:].reset_index(drop=True)

    # The first row contains column names - update it to the column
    df.columns = df.iloc[0]

    # Drop the first row as they are now column names
    df = df.drop(df.index[0]).reset_index(drop=True)
    
    # Add program
    cleaned_file_path = file_name.title().replace('_',' ')
    df['program'] = f"LOST {cleaned_file_path}"
    
    # Add county
    county = file_name.split("_")[0].replace("./","").title()
    df['county'] = county
    
    # Drop rows with more than 2 missing values
    df = df.dropna(axis = 0, thresh=2)
    
    # Add notes
    df['notes'] = notes

    return df 