import _utils
from shared_utils import utils
import geopandas as gpd
import numpy as np
import pandas as pd
from calitp_data_analysis.sql import to_snakecase

import fuzzywuzzy
from fuzzywuzzy import process

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

def row_limit(df, max_row_limit: int = 10000):
    """
    Print whether the dataframe is less
    or greater than the limit.
    """
    # Grab length
    df_length = len(df)
    
    if df_length >= max_row_limit:
        return f"{df_length} rows -> over max limit"
    else:
        return f"{df_length} rows -> under max limit"
    
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

"""
Merges
"""
def m_nonshopp_sb1_project_titles(nonshopp_df, sb1_df):
    """
    Merge nonshopp and SB1 on project titles.
    """
    sb1_subset = ['projecttitle_x','projectdescription','countynames','geometry']
    
    # Delete any rows without a project title
    sb1_df =  sb1_df.loc[sb1_df.projecttitle_x != "None"][sb1_subset]
    
    # Do an innter merge
    m1 = pd.merge(sb1_df,
    nonshopp_df,
    how="inner",
    left_on=[ "projecttitle_x"],
    right_on=["project_name",],
    indicator=True,)
    
    return m1

def replace_matches_in_column(df, column, new_col_name, string_to_match, min_ratio):
    """
    Replace all rows in agency column with a min ratio with  "string_to_match value"
    """
    # Get a list of unique strings
    strings = df[column].unique()

    # Get the top 10 closest matches to our input string
    matches = fuzzywuzzy.process.extract(
        string_to_match, strings, limit=10, scorer=fuzzywuzzy.fuzz.token_sort_ratio
    )

    # Only get matches with a  min ratio
    close_matches = [matches[0] for matches in matches if matches[1] > min_ratio]

    # Get the rows of all the close matches in our dataframe
    rows_with_matches = df[column].isin(close_matches)

    # replace all rows with close matches with the input matches
    df.loc[rows_with_matches, new_col_name] = string_to_match
    
def m_nonshopp_sb1_fuzzy(merge1, nonshopp_df, sb1_df):
    """
    Merge project titles using fuzzy matching
    """
    # List of projects that were already found
    found_projects = merge1.projecttitle_x.unique().tolist()
    
    # Filter out projects that were already found during the first merge
    # There's no point in merging everything again.
    sb1_df = sb1_df[~sb1_df["projecttitle_x"].isin(found_projects)].reset_index(drop = True)
    
    # Filter out projects that were already found during the first merge
    nonshopp_df = nonshopp_df[~nonshopp_df["project_name"].isin(found_projects)].reset_index(drop = True)
    
    # Place nonshopp projects into a list
    nonshopp_projects = nonshopp_df.project_name.unique().tolist()
    
    # Remove any short project titles that have less than 4 words
    sb1_df["projecttitle_x_count"] = sb1_df["projecttitle_x"].str.count('\w+')
    
    # Delete project titles that are short
    sb1_df = (sb1_df.loc[sb1_df.projecttitle_x_count > 3]).reset_index(drop = True)
    
    # Replace 
    for i in nonshopp_projects:
        replace_matches_in_column(
            sb1_df
            , "projecttitle_x", "project_title_fuzzy_match", i,90 
        )
    
    # Return only projects that match
    fuzzy_match_subset = ['projecttitle_x','project_title_fuzzy_match','projectdescription','countynames','geometry']
    fuzzy_match_results = (sb1_df.loc[sb1_df.project_title_fuzzy_match.notnull()]
                       .drop_duplicates(subset = ["projecttitle_x", "project_title_fuzzy_match", "projectdescription"])
                       .reset_index(drop = True)
                       .sort_values('projecttitle_x')
                      )
    
    # Last project isn't a match - delete it
    fuzzy_match_results = (fuzzy_match_results
                           .drop(columns = ["_merge"])
                           .head(4)) 
    
    # Subset columns
    fuzzy_match_results = fuzzy_match_results[fuzzy_match_subset]
    
    # Merge 
    merge1 = pd.merge(
    fuzzy_match_results,
    nonshopp_df,
    how="inner",
    left_on=["project_title_fuzzy_match", "countynames"],
    right_on=["project_name", "full_county_name"],)
    
    return merge1
    