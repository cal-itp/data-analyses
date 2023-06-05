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

def give_info(df, project_title: str, other_descriptive_col: str):
    """
    Print some basic information on the dataframe.
    """
    print(df[project_title].value_counts().head())
    print(f"# of unique project titles: {df[project_title].nunique()}")
    print(
        f"After dropping duplicates using {project_title} and {other_descriptive_col}: {len(df.drop_duplicates(subset = [project_title, other_descriptive_col]))}"
    )
    print(f"Df shape: {df.shape}")
    print(df.columns)
    
def embedded_column_names(df, data_start: int) -> pd.DataFrame:
    """
    Some excel sheets have headers and column names
    embedded in the dataframe. Take them out.
    
    Args:
        data_start: the row number the column names begin.
    """
    # Delete header
    df = df.iloc[data_start:].reset_index(drop=True)
    # The first row contains column names - update it to the column
    df.columns = df.iloc[0]

    # Drop the first row as they are now column names
    df = df.drop(df.index[0]).reset_index(drop=True)

    return df

def open_rest_server(url_pt_1: str, url_pt_2: str, layer_name: list):
    """
    Open up data that is availably publicly via ArcGis
    """
    full_gdf = pd.DataFrame()
    for i in layer_name:
        gdf = to_snakecase(gpd.read_file(f"{url_pt_1}{i}{url_pt_2}"))
        gdf["layer_name"] = i
        full_gdf = pd.concat([full_gdf, gdf], axis=0)

    return full_gdf

def delete_embedded_headers(df, column: str, string_search: str) -> pd.DataFrame:
    """
    Some PDFS include the column names embedded mulitple times
    within the df. Delete them out.
    
    Example: Under the column 'description', delete the rows
    in which the value is 'description.' This signals that the row
    is just repeating the column name again.
    """
    headers = df[df[column].str.contains(string_search) == True]
    headers_index_list = headers.index.values.tolist()

    print(f"{len(headers_index_list)} rows are headers")

    df2 = df.drop(headers_index_list).reset_index(drop=True)
    return df2

def correct_project_cost(df, project_title_col: str, project_total_cost: str):
    """
    For some datasets, the same project
    (as determined by the same project name, cost,
    and source) is split across multiple rows.

    Ex: A project costs $500 million and is
    split on 5 rows by phase/location. Each row still lists
    the total  cost as $500 million, which is not accurate.
    This function will recalculate each of the row to list
    $100 mil as the total project cost
    """
    # Create a unique identifier
    df["unique_identifier"] = df[project_title_col] + df[project_total_cost].astype(str)

    # Create count for each project
    df["how_many_times_same_proj_appears"] = (
        df.groupby("unique_identifier").cumcount() + 1
    )

    # Find the total number of times a project title-cost appears.
    # Sort by descending and keep only the row with the highest level
    df2 = (
        df[
            [
                project_title_col,
                "how_many_times_same_proj_appears",
                project_total_cost,
                "unique_identifier",
            ]
        ]
        .sort_values(
            [project_title_col, "how_many_times_same_proj_appears"], ascending=False
        )
        .drop_duplicates(subset=["unique_identifier"])
    )
    # Create new funding estimate
    df2["new_proj_cost"] = (
        df2[project_total_cost] / df2["how_many_times_same_proj_appears"]
    )

    # Drop some columns
    df2 = df2.drop(
        columns=[
            project_title_col,
            project_total_cost,
            "how_many_times_same_proj_appears",
        ]
    )

    # Merge
    m1 = pd.merge(df, df2, how="inner", on="unique_identifier")

    # Clean up
    m1 = m1.drop(
        columns=[
            "unique_identifier",
            "how_many_times_same_proj_appears",
            project_total_cost,
        ]
    )
    m1[
        "total_project_cost_note"
    ] = "This is an estimate of how much the project cost, estimated by dividing the total project cost by how many times the project appears in the dataset."

    # Replace project cost
    m1 = m1.rename(columns={"new_proj_cost": "total_project_cost"})
    return m1