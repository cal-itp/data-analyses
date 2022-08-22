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

# Remove underscores, title case, and strip whitespaces
def clean_up_columns(df):
    df.columns = df.columns.str.replace("_", " ").str.title().str.strip()
    return df

# Function that moves all elements in a particular column that is separated by commas onto one line 
def service_comps_summarize(df, components_wanted: list):

    # Filter out
    df = df[df["service_components_component_name"].isin(components_wanted)]

    # Puts all the components  an organization purchases onto one line, instead of a few different ones into a df
    comps = summarize_rows(
        df, "service_components_service_name", "service_components_component_name"
    )

    # Puts all product names onto one line into a new df
    prods = summarize_rows(
        df, "service_components_service_name", "service_components_product_name"
    )

    # Merge comps and prods together
    m1 = pd.merge(comps, prods, how="inner", on="service_components_service_name")

    return m1