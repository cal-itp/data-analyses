import numpy as np
import pandas as pd
import shared_utils
from calitp_data_analysis.sql import to_snakecase
from dgs_data_cleaner import new_bus_size_finder, new_prop_finder
from tircp_data_cleaner import col_row_updater

gcs_path = "gs://calitp-analytics-data/data-analyses/bus_procurement_cost/"

def col_splitter(
    df: pd.DataFrame, 
    col_to_split: str, 
    new_col1: str, 
    new_col2: str, 
    split_char: str
)-> pd.DataFrame:
    """
    function to split a column into 2 columns by specific character.
    ex. split 100(beb) to "100" & "(beb)"
    """
    df[[new_col1, new_col2]] = df[col_to_split].str.split(
        pat=split_char, n=1, expand=True
    )

    df[new_col2] = df[new_col2].str.replace(")", "")

    return df


def agg_just_bus(df: pd.DataFrame) -> pd.DataFrame:
    """
    filters FTA data to only show projects with bus procurement (bus count > 0).
    """
    df1 = df[df["bus_count"] > 0]

    df2 = (
        df1.groupby(
            [
                "project_sponsor",
                "project_title",
                "new_prop_type_finder",
                "new_bus_size_type",
            ]
        )
        .agg(
            {
                "funding": "sum",
                "bus_count": "sum",
            }
        )
        .reset_index()
    )

    return df2


def clean_fta_columns() -> pd.DataFrame:
    """
    Main function to clean FTA data. Reads in data, changes datatypes, change specific values.
    """
    # params
    
    file = "data-analyses_bus_procurement_cost_fta_press_release_data_csv.csv"

    # read in data
    df = pd.read_csv(f"{gcs_path}{file}")

    # snakecase df
    df = to_snakecase(df)

    # clean funding values
    df["funding"] = (
        df["funding"]
        .str.replace("$", "")
        .str.replace(",", "")
        .str.strip()
    )

    # rename initial propulsion type col to propulsion category
    df = df.rename(columns={"propulsion_type": "prosulsion_category"})

    # splittign `approx_#_of_buses col to get bus count
    df1 = col_splitter(df, "approx_#_of_buses", "bus_count", "extract_prop_type", "(")

    # assign new columns via new_prop_finder and new_bus_size_finder
    df2 = df1.assign(
        new_prop_type_finder=df1["description"].apply(new_prop_finder),
        new_bus_size_type=df1["description"].apply(new_bus_size_finder),
    )

    # cleaning specific values
    col_row_updater(df2, "funding", "7443765", "bus_count", 56)
    col_row_updater(df2, "funding", "17532900", "bus_count", 12)
    col_row_updater(df2, "funding", "40402548", "new_prop_type_finder", "CNG")
    col_row_updater(df2, "funding", "30890413", "new_prop_type_finder", "mix (zero and low emission)")
    col_row_updater(df2, "funding", "29331665", "new_prop_type_finder", "mix (zero and low emission)")
    col_row_updater(df2, "funding", "7598425", "new_prop_type_finder", "mix (zero and low emission)")
    col_row_updater(df2, "funding", "7443765", "new_prop_type_finder", "mix (zero and low emission)")
    col_row_updater(df2, "funding", "3303600", "new_prop_type_finder", "mix (diesel and gas)")
    col_row_updater(df2, "funding", "2063160", "new_prop_type_finder", "low emission (hybrid)")
    col_row_updater(df2, "funding", "1760000", "new_prop_type_finder", "low emission (propane)")
    col_row_updater(df2, "funding", "1006750", "new_prop_type_finder", "ethanol")
    col_row_updater(df2, "funding", "723171", "new_prop_type_finder", "low emission (propane)")
    col_row_updater(df2, "funding", "23280546", "new_prop_type_finder", "BEB")

    # update data types
    update_cols = ["funding", "bus_count"]

    df2[update_cols] = df2[update_cols].astype("int64")

    return df2


if __name__ == "__main__":

    # initial df (all projects)
    all_projects = clean_fta_columns()

    # projects with bus count > 0 only.
    just_bus = agg_just_bus(all_projects)

    # export both DFs
    all_projects.to_parquet(f"{gcs_path}fta_all_projects_clean.parquet")
    just_bus.to_parquet(f"{gcs_path}fta_bus_cost_clean.parquet")