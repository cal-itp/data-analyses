"""
These functions are to help search through lists
for specific types of projects
"""
import pandas as pd
import geopandas as gpd
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

def filter_projects(
    df,
    columns_to_search: list,
    keywords_search: list,
    file_name: str,
    save_to_gcs: bool = True,
):

    # Filter out for Cordon
    df = find_keywords(df, columns_to_search, keywords_search)
    df2 = (
        df[
            (df.lower_case_project_title_keyword_search != "keyword not found")
            | (df.lower_case_project_description_keyword_search != "keyword not found")
        ]
    ).reset_index(drop=True)

    # Delete out non HOV projects that were accidentally picked up
    projects_to_delete = [
        "SR 17 Corridor Congestion Relief in Los Gatos",
        "Interstate 380 Congestion Improvements",
    ]
    df2 = df2[~df2.project_title.isin(projects_to_delete)].reset_index(drop=True)

    # Change cases
    for i in ["project_title", "project_description"]:
        df2[i] = df2[i].str.title()

    # Drop invalid geometries
    gdf = df2[df2.geometry != None].reset_index(drop=True)
    gdf = gdf.set_geometry("geometry")
    gdf.geometry = gdf.geometry.set_crs("EPSG:4326")
    gdf = gdf[gdf.geometry.is_valid].reset_index(drop=True)
    gdf = gdf.fillna(gdf.dtypes.replace({"float64": 0.0, "object": "None"}))

    # One version that's a df
    columns_to_drop = ["lower_case_project_title", "lower_case_project_description"]
    df2 = df2.drop(columns=columns_to_drop + ["geometry"])
    df2 = df2.fillna(df.dtypes.replace({"float64": 0.0, "object": "None"}))

    if save_to_gcs:
        df2.to_excel(
            f"{harmonization_utils.GCS_FILE_PATH}LRTP/{file_name}.xlsx",
            index=False,
        )
        gdf.to_file(f"./{file_name}.geojson", driver="GeoJSON")

    return gdf, df2