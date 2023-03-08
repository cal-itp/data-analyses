import camelot
import pandas as pd

def open_pdf(file_name:str, pages: str):
    """
    Returns concatted dataframe 
    extracted from a PDF. 
    pip install "camelot-py[base]"
    
    file_path (str): PDF must be saved locally in the 
    directory. Ex: "./alameda_b_2000.pdf"
    
    pages (str): The pages the tables are located in. 
    Ex: "5,6,7" or "41,41". 
    """
    opened_pdf = camelot.read_pdf(f"./{file_name}.pdf", pages = pages,flavor='stream', strip_text='.\n', edge_tol=300)
    
    # Concat the different tables into one dataframe
    # https://stackoverflow.com/questions/62044535/how-to-extract-tables-from-pdf-using-camelot
    final = pd.DataFrame()
    for page, pdf_table in enumerate(opened_pdf):           
        table = opened_pdf[page].df
        final = pd.concat([final, table], axis=0)
    
    return final 

def clean_pdf(df, row_to_keep:int, 
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