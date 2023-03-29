import _utils
from shared_utils import utils
import geopandas as gpd
import numpy as np
import pandas as pd
from calitp_data_analysis.sql import to_snakecase

"""
Basic Cleaning
"""
def basic_cleaning(df, agency_col: str,
                   project_name_col:str,
                   project_id_col: str, 
                   project_desc_col:str):
    """
    Perform basic cleaning before joining 
    SB1 & Non SHOPP data together.
    """
    # Clean agency names.
    df = _utils.organization_cleaning(df, agency_col)
    
    # Remove all punctation, lowercase, and strip whitespaces from 
    # project titles & descriptions. Count number of strings.
    for i in [project_name_col, project_desc_col]:
        df[i] = (df[i].str.lower().str.replace('[^\w\s]','').str.strip())
        df[f"{i}_count"] = df[i].str.count('\w+')
                 
    # Some project names contain the year. Remove anything after 20..
    df[project_name_col] = df[project_name_col].str.split("20").str[0]
    
    # Project ID, remove all commas and lowercase if there are strings
    df[project_id_col] = (df[project_id_col].str.replace("'", "").str.lower().str.strip())
    
    # Get rid of | in object cols
    # https://stackoverflow.com/questions/68152902/extracting-only-object-type-columns-in-a-separate-list-from-a-data-frame-in-pand
    string_cols = df.select_dtypes(include=['object']).columns.to_list()
    try:
        for i in string_cols:
            df[i] = df[i].str.replace("|", "")
    except:
        pass
    
    # Try to extract titles from popups
    try:
        df["popup"] = df['popup'].str.split("<br  />").str[1].str.split("20").str[0].str.lower().str.strip().str.replace('[^\w\s]','')
    except:
        pass
 
    return df

"""
Non Shopp
"""
def load_nonshopp(file_path: str):
    """
    Load complete Non-SHOPP projects and 
    the 9 sample projects.
    """
    # Read in 10 Year non SHOPP with ATP and TIRCP
    df = to_snakecase(pd.read_excel(f"{_utils.GCS_FILE_PATH}{file_path}"))
    
    # Add a zero in front of single digits
    df.district = df.district.map("{:02}".format)
    
    # Do basic cleaning
    df = basic_cleaning(df, "lead_agency", "project_name", "ct_project_id", "project_description")
    
    # Return a dataframe with the 9 sample projects
    nine_projects_id = [
    "0422000202",
    "0414000032",
    "0520000083",
    "0515000063",
    "0721000056",
    "0716000370",
    "0813000222",
    "0814000144",
    "0414000032",
    "0720000165",]
        
    sample_projects = (df[df.ct_project_id.isin(nine_projects_id)].reset_index(drop=True))
    
    return df, sample_projects

"""
SB1
"""
url_pt1 = "https://odpsvcs.dot.ca.gov/arcgis/rest/services/RCA/RCA_Projects_032022/FeatureServer/"
url_pt2 = "/query?where=1%3D1&objectIds=&time=&geometry=&geometryType=esriGeometryEnvelope&inSR=&spatialRel=esriSpatialRelIntersects&distance=&units=esriSRUnit_Foot&relationParam=&outFields=*+&returnGeometry=true&maxAllowableOffset=&geometryPrecision=&outSR=&gdbVersion=&historicMoment=&returnDistinctValues=false&returnIdsOnly=false&returnCountOnly=false&returnExtentOnly=false&orderByFields=&groupByFieldsForStatistics=&outStatistics=&returnZ=false&returnM=false&multipatchOption=&resultOffset=&resultRecordCount=&returnTrueCurves=false&sqlFormat=none&f=geojson"

def load_sb1_rest_server() -> gpd.GeoDataFrame:
    """
    Load all the projects on the SB1
    map from the Feature Server. 
    
    https://odpsvcs.dot.ca.gov/arcgis/rest/services/RCA/RCA_Projects_032022/FeatureServer
    """
    full_gdf = pd.DataFrame()
    for i in [*range(0,22)]:
        df = to_snakecase(gpd.read_file(f"{url_pt1}{i}{url_pt2}"))
        full_gdf = pd.concat([full_gdf, df], axis=0)
        
    
    # Basic cleaning
    full_gdf = basic_cleaning(full_gdf, 'agencies','projecttitle','projectid',
                         'projectdescription')
    
    # Fill in project titles that are empty with information
    # gleaned from the pop up. 
    full_gdf['projecttitle'] = full_gdf['projecttitle'].fillna(full_gdf['popup'])
    
    # Throw out missing geometry
    missing_geo = full_gdf[full_gdf.geometry.is_empty]
    full_gdf = full_gdf[~full_gdf.geometry.is_empty].reset_index(drop = True)
    
    return full_gdf, missing_geo

def load_sb1_all_projects():
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
    df = basic_cleaning(df, 'implementingagency','projecttitle','projectid',
                         'projectdescription')
    
    return df

def sb1_final() -> gpd.GeoDataFrame:
    """
    Layers 0-21 with geographic information
    don't always have project titles for each
    of the projects. Merge these layers with 
    layer 22, which does have title information.

    """
    all_projects_subset = ['projecttitle', "programcodes", "totalcost", "implementingagency", "fiscalyearcode"]
    
    sb1_geo, missing_geo = load_sb1_rest_server()
    sb1_all_projects = load_sb1_all_projects()[all_projects_subset]
    
    # Merge
    merge1 = pd.merge(
    sb1_geo,
    sb1_all_projects,
    how="left",
    left_on=[ "programcodes", "totalcost", "agencies","fiscalyearcodes"],
    right_on=[ "programcodes", "totalcost", "implementingagency","fiscalyearcode"],
    indicator = True)
    
    # Fill in missing project titles in sb1_geo with information from
    # sb1_all_projects
    merge1.projecttitle_x = merge1.projecttitle_x.fillna(merge1.projecttitle_y)
    
    # Fill missing titles with none
    merge1.projecttitle_x = merge1.projecttitle_x.fillna('None')
    
    return merge1
    