import pandas as pd
from calitp_data_analysis.sql import to_snakecase

GCS_FILE_PATH = "gs://calitp-analytics-data/data-analyses/project_list/"

"""
General use functions
for cleaning grants data.
"""
def clean_columns(df, strings_to_remove:list):
    """
    Remove certain strings from columns.
    
    Example: I want to remove 'tircp_' 
    before all the column names.
    """
    strings_to_remove = r"({})".format("|".join(strings_to_remove))
    
    df.columns = df.columns.str.replace(strings_to_remove, "", regex=True)
    
    return df 


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
