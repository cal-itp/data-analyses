import geopandas as gpd
import numpy as np
import pandas as pd
import _harmonization_utils 
from calitp_data_analysis.sql import to_snakecase
from calitp_data_analysis import utils

url_pt1 = "https://odpsvcs.dot.ca.gov/arcgis/rest/services/RCA/RCA_Projects_032022/FeatureServer/"
url_pt2 = "/query?where=1%3D1&objectIds=&time=&geometry=&geometryType=esriGeometryEnvelope&inSR=&spatialRel=esriSpatialRelIntersects&distance=&units=esriSRUnit_Foot&relationParam=&outFields=*+&returnGeometry=true&maxAllowableOffset=&geometryPrecision=&outSR=&gdbVersion=&historicMoment=&returnDistinctValues=false&returnIdsOnly=false&returnCountOnly=false&returnExtentOnly=false&orderByFields=&groupByFieldsForStatistics=&outStatistics=&returnZ=false&returnM=false&multipatchOption=&resultOffset=&resultRecordCount=&returnTrueCurves=false&sqlFormat=none&f=geojson"

def sb1_basic_cleaning(
    df,
    agency_col: str,
    project_name_col: str,
    project_id_col: str,
    project_desc_col: str,
):
    """
    Perform basic cleaning before joining
    SB1 & Non SHOPP data together.
    """
    # Some project names contain the year. Remove anything after 20..
    df[project_name_col] = df[project_name_col].str.split("20").str[0]

    # Get rid of | in object cols
    # https://stackoverflow.com/questions/68152902/extracting-only-object-type-columns-in-a-separate-list-from-a-data-frame-in-pand
    string_cols = df.select_dtypes(include=["object"]).columns.to_list()
    try:
        for i in string_cols:
            df[i] = df[i].str.replace("|", "")
    except:
        pass

    # Try to extract titles from popups
    try:
        df["popup"] = (
            df["popup"]
            .str.split("<br  />")
            .str[1]
            .str.split("20")
            .str[0]
            .str.lower()
            .str.strip()
            .str.replace("[^\w\s]", "")
        )
    except:
        pass

    return df

def load_sb1_rest_server() -> gpd.GeoDataFrame:
    """
    Load all the projects on the SB1
    map from the Feature Server.

    https://odpsvcs.dot.ca.gov/arcgis/rest/services/RCA/RCA_Projects_032022/FeatureServer
    """
    full_gdf = pd.DataFrame()
    for i in [*range(0, 22)]:
        df = to_snakecase(gpd.read_file(f"{url_pt1}{i}{url_pt2}"))
        full_gdf = pd.concat([full_gdf, df], axis=0)

    # Basic cleaning
    full_gdf = sb1_basic_cleaning(
        full_gdf, "agencies", "projecttitle", "projectid", "projectdescription"
    )

    # Fill in project titles that are empty with information
    # gleaned from the pop up.
    full_gdf["projecttitle"] = full_gdf["projecttitle"].fillna(full_gdf["popup"])

    # Throw out missing geometry
    missing_geo = full_gdf[full_gdf.geometry.is_empty]
    full_gdf = full_gdf[~full_gdf.geometry.is_empty].reset_index(drop=True)

    return full_gdf, missing_geo

def load_sb1_all_projects() -> pd.DataFrame:
    """
    Load in all projects layer of SB1 because it
    contains a value for every row in the
    project title column.
    """
    df = f"{url_pt1}22{url_pt2}"

    df = to_snakecase(gpd.read_file(df))

    # No geometry, just drop it
    df = df.drop(columns=["geometry"])

    # Basic cleaning
    df = sb1_basic_cleaning(
        df, "implementingagency", "projecttitle", "projectid", "projectdescription"
    )

    return df

def sb1_final() -> gpd.GeoDataFrame:
    """
    Layers 0-21 with geographic information
    don't always have project titles for each
    of the projects. Merge these layers with
    layer 22, which does have title information.
    """
    all_projects_subset = [
        "projecttitle",
        "programcodes",
        "totalcost",
        "implementingagency",
        "fiscalyearcode",
    ]

    sb1_geo, missing_geo = load_sb1_rest_server()
    sb1_all_projects = load_sb1_all_projects()[all_projects_subset]

    # Merge
    merge1 = pd.merge(
        sb1_geo,
        sb1_all_projects,
        how="left",
        left_on=["programcodes", "totalcost", "agencies", "fiscalyearcodes"],
        right_on=["programcodes", "totalcost", "implementingagency", "fiscalyearcode"],
    )

    # Fill in missing project titles in sb1_geo with information from
    # sb1_all_projects
    merge1.projecttitle_x = merge1.projecttitle_x.fillna(merge1.projecttitle_y)

    # Fill missing titles with none
    merge1.projecttitle_x = merge1.projecttitle_x.fillna("None")
    
    # Drop columns
    merge1 = merge1.drop(columns = ['projecttitle_y'])
    
    # Filter out completed projects
    merge1 = merge1[merge1.projectstatuscodes != 'Completed'].reset_index(drop = True)
    
    merge1 = merge1.fillna(merge1.dtypes.replace({'float64': 0.0, 'object': 'None'}))
    
    return merge1