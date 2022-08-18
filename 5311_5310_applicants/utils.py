import pandas as pd
from calitp import *

# Clean organization names 
def organization_cleaning(df, column_wanted: str):
    df[column_wanted] = (
        df[column_wanted]
        .str.strip()
        .str.split(",")
        .str[0]
        .str.replace("/", "")
        .str.split("(")
        .str[0]
        .str.split("/")
        .str[0]
    )
    return df

'''
Puts all the elements in the column "col to summarize" onto one line and separates them by commas. 
For ex: an agency typically purchases 1+ products but in the original dataset,
each product has its own line: this function can grab all the products by agency and puts them on the same line.
'''
def summarize_rows(df, col_to_group: str, col_to_summarize: str):
    df_col_to_summarize = (df
    .groupby(col_to_group)[col_to_summarize]
    .apply(','.join)
    .reset_index()
     )
    return df_col_to_summarize
