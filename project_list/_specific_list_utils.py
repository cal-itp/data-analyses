"""
These functions are to help search through lists
for specific types of projects
"""
import pandas as pd
from calitp_data_analysis.sql import to_snakecase
import _harmonization_utils as harmonization_utils


def lower_case(df, columns_to_search: list):
    """
    Lowercase and clean certain columns:
    """
    new_columns = []
    for i in columns_to_search:
        df[f"lower_case_{i}"] = (
            df[i]
            .str.lower()
            .fillna("none")
            .str.replace("-", "")
            .str.replace(".", "")
            .str.replace(":", "")
        )
        new_columns.append(f"lower_case_{i}")

    return df, new_columns

def find_keywords(df, columns_to_search: list, keywords_search: list):
    """
    Find keywords in certain columns
    """
    df2, lower_case_cols_list = lower_case(df, columns_to_search)

    keywords_search = f"({'|'.join(keywords_search)})"

    for i in lower_case_cols_list:
        df2[f"{i}_keyword_search"] = (
            df2[i].str.extract(keywords_search).fillna("keyword not found")
        )

    return df2

