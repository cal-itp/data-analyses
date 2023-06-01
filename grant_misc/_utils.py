import fuzzywuzzy
import pandas as pd
import siuba  # need this to do type hint in functions
from calitp_data_analysis.sql import to_snakecase
from calitp_data_analysis.tables import tbls
from fuzzywuzzy import process
from siuba import *


gcs_path = "gs://calitp-analytics-data/data-analyses/grant_misc/"

"""
General Functions
"""
def summarize_rows(df, col_to_group: str, col_to_summarize: str) -> pd.DataFrame:
    """
    Puts all the elements in the column "col to summarize"
    onto one line and separates them by commas.
    
    Example: Agency ABC applied to 5311, 5310, and
    5311(f) but each program has its own line. 
    Group all the program information (col_to_summarize) by
    the agency (col_to_group), so Agency ABC will have all 
    its program information in one row. 
    
    Args:
        df: dataframe
        col_to_group (str): the column to group
        col_to_summarize (str): the column to "unexplode."
    """
    df = df.groupby(col_to_group)[col_to_summarize].apply(",".join).reset_index()
    return df

def delete_repeated_element(df, col: str):
    """
    If an element is repeated more than once and delinated by commas
    in a column, delete the duplicative ones.
    
    Ex: the column "grocery_list" has apples, cherries, cheese, and apples.
    Keep apples only once. 
    """
    df[col] = (
        df[col]
        .apply(lambda x: ", ".join(set([y.strip() for y in x.split(",")])))
        .str.strip()
    )
    return df

def clean_punctuation(df, agency_col: str) -> pd.DataFrame:
    """
    Cleans up agency names. Assume anything after comma/()/
    ; are acronyms and delete them, correct certain mispellings.
    Change agency names to title case, clean whitespaces.
    
    Args:
        df: dataframe
        agency_col (str): column that contains lead agency/organization
    """
    df[agency_col] = (
        df[agency_col]
        .str.strip()
        .str.split(",")
        .str[0]
        .str.replace("/", "")
        .str.split("(")
        .str[0]
        .str.split("/")
        .str[0]
        .str.split(";")
        .str[0]
        .str.title()
        .str.replace("Trasit", "Transit")
        .str.replace("*", "")
        .str.replace("Agency", "")
        .str.strip()  # strip whitespaces again after getting rid of certain things
    )
    return df

def flip_county_city(df, agency_col: str)->pd.DataFrame:
    """
    Change County of Sonoma to Sonoma County,
    City of Berkeley to Berkely City. 
    
    Used Natalie's function:  https://github.com/cal-itp/data-analyses/blob/main/Agreement_Overlap/add_dla.ipynb
    
    Args:
        df: dataframe
        agency_col (str): column that contains lead agency/organization
    """
    to_correct = df[
        (df[agency_col].str.contains("County")) | (df[agency_col].str.contains("City"))
    ]
    to_correct = to_correct[[agency_col]].drop_duplicates().reset_index(drop=True)
    to_correct["str_len"] = to_correct[agency_col].str.split().str.len()
    to_correct = to_correct[to_correct.str_len <= 5].reset_index(drop=True)
    to_correct[["name_pt1", "name_pt2"]] = to_correct[agency_col].str.split(
        " Of ", 1, expand=True
    )
    to_correct["new_name"] = to_correct["name_pt2"] + " " + to_correct["name_pt1"]

    new_names_dictionary = dict(to_correct[[agency_col, "new_name"]].values)
    df["agency_corrected"] = df[agency_col].map(new_names_dictionary)
    df["agency_corrected"] = df["agency_corrected"].fillna(df[agency_col])

    df = df.drop(columns=[agency_col])
    df = df.rename(columns={"agency_corrected": agency_col})

    return df

def clean_organization_names(df, agency_col: str) -> pd.DataFrame:
    """
    Clean up organization name columns. 
    
    Args:
        df: dataframe
        agency_col (str): column that contains lead agency/organization
    """
    df = clean_punctuation(df, agency_col)
    df = flip_county_city(df, agency_col)
    return df

def replace_matches_set_ratio(df, column, new_col_name, string_to_match, min_ratio):

    # Get a list of unique strings
    strings = df[column].unique()

    # Get the top 10 closest matches to our input string
    matches = fuzzywuzzy.process.extract(
        string_to_match, strings, limit=10, scorer=fuzzywuzzy.fuzz.token_set_ratio
    )

    # Only get matches with a  min ratio
    close_matches = [matches[0] for matches in matches if matches[1] > min_ratio]

    # Get the rows of all the close matches in our dataframe
    rows_with_matches = df[column].isin(close_matches)

    # replace all rows with close matches with the input matches
    df.loc[rows_with_matches, new_col_name] = string_to_match
    
def find_fuzzy_match(
    df1,
    df2,
    df1_fuzzy_column: str,
    df2_fuzzy_column: str,
    new_column: str,
    min_ratio: int)->pd.DataFrame:
    """
    Using the fuzzywuzzy package, find matches
    between two columns and two dataframes.
    
    Ex: df1 lists "AC Transit" & df2 lists "Alameda Contra Costa Transit."
    Assume we want to use df1 as the "source of truth", a new column in df2
    will populate with "AC Transit" adjacent to df2's original name.
    
     Args:
        df1: df that is the source of truth
        df2: df that you want to match values against df1. 
        df1_fuzzy_column (str): df1's column to fuzzy match
        df2_fuzzy_column (str): df2's column to fuzzy match
        new_column (str): instead of replacing values in df2, place
        potential matches in a new column
        min_ratio (int): the accuracy of the match, the higher, the more 
        accurate
        
    """
    unique_values = df1[df1_fuzzy_column].unique().tolist()
    for i in unique_values:
        replace_matches_set_ratio(df2, df2_fuzzy_column, new_column, i, min_ratio)
    return df2

"""
Dictionary
"""
to_map = {
    "Tulare County": "Tuolumne County Transit",
    "Turlock City": "Turlock Transit",
    "Union City City": "Union City Transit",
    "Calaveras Transit": "Calaveras Connect",
    "Alameda-Contra Costa Transit District": "Ac Transit",
    "Arcadia City": "Arcadia Transit",
    "Banning City": "Banning Pass Transit",
    "Beaumont City": "Beaumont Pass Transit",
    "Calaveras Council Of Governments": "Calaveras Connect",
    "Camarillo City": "Camarillo Area Transit",
    "Commerce City": "Commerce Municipal Bus Lines",
    "Corona City": "Corona Cruiser",
    "Delano City": "Delano Area Rapid Transit",
    "Eastern Sierra Transit Authority": "Eastern Sierra Transit Authority Community Routes",
    "Elk Grove City": "Elk Grove Transit Services",
    "Fairfield City": "Fairfield And Suisun Transit",
    "Folsom City": "Folsom Stage Line",
    "Glenn County": "Glenn Ride",
    "Guadalupe City": "Guadalupe Flyer",
    "Lassen County": "Lassen Rural Bus",
    "Marin County Transit District": "Marin Transit",
    "Madera County": "Madera Metro",
    "Mariposa County": "Mariposa Grove Shuttle",
    "Morro Bay City": "Morro Bay Transit",
    "Norwalk City": "Norwalk Transit System",
    "Roseville City": "Roseville Transit",
    "Sacramento Regional Transit District": "Sacramento Regional Transit District Bus",
    "San Diego City": "San Diego Trolley",
    "San Francisco City": "Muni Bus",
    "Santa Rosa City": "Santa Rosa Citybus",
    "Shafter City": "Shafter Dial-A-Ride",
    "Sierra County": "Sierra Point Shuttle",
    "Simi Valley City": "Simi Valley Transit",
    "Sonoma Marin Area Rail Transit": "Sonoma-Marin Area Rail Transit",
    "Arvin City": "Arvin Transit",
    "Auburn City": "Auburn Transit",
    "County Of Los Angeles - Department Of Public Works": "Los Angeles County Transit Services",
    "County Of Sacramento Department Of Transportation": "Sacrt Bus",
    "Dinuba City": "Dinuba Connection",
    "Lassen Transit Service": "Lassen Rural Bus",
    "Needles City": "Needles Area Transit",
    "Nevada Public Works": "County Nevada County Connects",
    "Ojai City": "Ojai Trolley",
    "Palo Verde Valley Transit": "Palos Verdes Peninsula Transit Authority",
    "Placer County Public Works": "Placer County Transit",
    "Plumas County Transportation Commission": "Plumas Transit Systems",
    "Porterville City": "Porterville Transit",
    "Ridgecrest City": "Ridgecrest Transit",
    "Rio Vista City": "Rio Vista Delta Breeze",
    "Santa Maria City": "Santa Maria Regional Transit",
    "Siskiyou County": "Siskiyou Transit And General Express",
    "Taft City": "Taft Area Transit",
    "Tehama County Transit": "Tehama Rural Area Express",
    "Transportation Trinity County Department": "Trinity Transit",
    "Transit Joint Powers Authority For Merced": "County Merced The Bus",
    "Visalia City": "Visilia Transit",
    "Yolo County Transportation District": "Yolobus",
}

"""
Blackcat
"""
def blackcat_orgs(file_name:str, year_wanted: int, grants_wanted: list)-> pd.DataFrame:
    """
    Open and filter blackcat file for the grant applicants. 
    
    Args:
        file_name (str): include .xlsx extension
        year_wanted (int): filter for records beyond a certain year
        grants_wanted (list): list of grant programs to subset
    """
    df = to_snakecase(pd.read_excel(f"{gcs_path}{file_name}"))
    
    # Filter grant fiscal year
    df = df[df.grant_fiscal_year >= 2018].reset_index(drop=True)
    
    df = df[df.funding_program.isin(grants_wanted)].reset_index(drop = True)
    
    # Cols
    subset = ["organization_name", "grant_fiscal_year", "funding_program"]
    sort_cols = ["organization_name", "funding_program"]
    
    # Summarize df so one row will correspond with one organization
    df = (df[subset]
    .sort_values(by=["organization_name", "grant_fiscal_year"], ascending=[True, False])
    .drop_duplicates(subset=sort_cols)
    .reset_index(drop=True)
    )
    
    df = summarize_rows(df, ["organization_name", "grant_fiscal_year"], "funding_program")
    
    # Drop extra rows
    df = (df
    .sort_values(by=sort_cols, ascending=[True, False])
    .drop_duplicates(subset=["organization_name"])
    .reset_index(drop=True))
    return df 

"""
State of Good Repair
"""
def sgr_orgs(file_name:str) ->  pd.DataFrame:
    """
    Open and filter State of Good Repair file for the grant applicants. 
    
    Args:
        file_name (str): include excel extension
    """
    df = to_snakecase(pd.read_excel(f"{gcs_path}{file_name}"))
    
    # Subset
    sgr_subset = ["first_name", "last_name", "email", "phone", "title", "agency"]
    df = df[sgr_subset]
    
    # Keep only one row for each agency
    df = df.drop_duplicates("agency").reset_index(drop=True)
    
    # Col to specify this is State of Good Repair data
    df["funding_program"] = "State of Good Repair"
    
    return df

"""
Airtable
"""
def airtable_service_type()->pd.DataFrame:
    """
    Load and clean Airtable Transit Services 
    service_type data via the warehouse. 
    This will return a df where one organization has one row. 
    """
    df = tbls.external_airtable.california_transit__services() >> collect()
    
    airtable_subset = ["name", "service_type"]
    
    df = df[airtable_subset]
    
    # Service types nested in a list. explode out
    df = df.explode("service_type").reset_index(drop=True)
    
    df = df.drop_duplicates().reset_index(drop = True)
    
    # Summarize so one row contains one organization and all funding programs
    # it has applied for.
    df = df.fillna('No Service Info')
    df = summarize_rows(df, ["name"], "service_type")
    
    return df

"""
A4_blackcat_recipients
"""
def fuzzy_match_sgr_bc(blackcat_df: pd.DataFrame, sgr_df:pd.DataFrame, matches_to_delete:list) -> pd.DataFrame:
    """
    Fuzzy match blackcat data against state of good repair. 
    """
    blackcat_df = clean_organization_names(blackcat_df, "organization_name")
    sgr_df = clean_organization_names(sgr_df, "agency")
    
    # sgr_df is the "source of truth" for organization name
    # goal is to change the names within blackcat_df
    matches = find_fuzzy_match(
    sgr_df, blackcat_df, "agency", "organization_name", "fuzzy_match_agency", 95
    )
    
    # Some matches are not correct, set them as none
    for i in matches_to_delete:
        matches.loc[matches["organization_name"].eq(i), "fuzzy_match_agency"] = None
    
    # Fill in any organizations that didn't get a match
    matches.fuzzy_match_agency = matches.fuzzy_match_agency.fillna(matches.organization_name)
    
    return matches

def merge_blackcat_sgr(blackcat_df:pd.DataFrame, sgr_df:pd.DataFrame)->pd.DataFrame:
    """
    After fuzzy matching, merge Black Cat and 
    State of Good Repair data.
    
    Args:
        blackcat_df: blackcat df, AFTER applying fuzzy_match_sgr_bc()
        sgr_df: SGR df, original.
    """
    # Merge
    m1 = pd.merge(
    blackcat_df,
    sgr_df,
    left_on=["fuzzy_match_agency"],
    right_on=["agency"],
    how="outer",
    indicator=True,)
    
    # Fill the funding programs that are empty
    m1.funding_program_x = m1.funding_program_x.fillna("State of Good Repair")
    m1.funding_program_y = m1.funding_program_y.fillna(m1.funding_program_x)
    
    # Combine the two funding programs since an applicant
    # can appear in both Blackcat and SGR
    m1["funding_program"] = m1.funding_program_x + "," + m1.funding_program_y
    m1.funding_program = m1.funding_program.fillna(m1.funding_program_y)
    
    # Fill in organization name that are null. These rows are SGR ones
    m1.organization_name = m1.organization_name.fillna(m1.agency)
    
    # Subset
    cols_to_drop = [
    "funding_program_x",
    "funding_program_y",
    "fuzzy_match_agency",
    "agency",
    "grant_fiscal_year"]
    
    m1 = m1.drop(columns=cols_to_drop)
    
    # Summarize so one row contains one organization and all funding programs
    # it has applied for.
    m2 = summarize_rows(m1,
    ["organization_name"],
    "funding_program")
    
    m2 = pd.merge(m2, m1.drop(columns = ['funding_program']), on = ['organization_name'], how = "left")
    m2 = m2.sort_values(['organization_name']).drop_duplicates(subset = ['organization_name'])
    return m2

def fuzzy_match_airtable_bc(airtable: pd.DataFrame, merged_blackcat_sgr:pd.DataFrame, wrong_matches:list)->pd.DataFrame:
    """
    Use the fuzzywuzzy package to match the organization names
    the dataframe that merged blackcat with SGR information
    against airtable agencies.
    
    Args:
        airtable (df): df from airtable_service_type()
        merged_blackcat_sgr(df): df from merge_blackcat_sgr()
        wrong_matches (list): fuzzy matching isn't always correct, so this list
        deletes out the incorrect matches.

    """

    merged_blackcat_sgr = find_fuzzy_match(airtable, merged_blackcat_sgr, "name", "organization_name", "fuzzy_agency", 95)
    
    for i in wrong_matches:
        merged_blackcat_sgr.loc[merged_blackcat_sgr["organization_name"].eq(i), "fuzzy_agency"] = None
        
    # These are the fuzzy matches that worked.
    found_matches = (merged_blackcat_sgr[((~merged_blackcat_sgr.fuzzy_agency.isna()) 
                    & (~merged_blackcat_sgr.fuzzy_agency.isin(wrong_matches)))]).reset_index(drop=True)
    
    # Organizations that still need matches
    still_need_matches = merged_blackcat_sgr[(~merged_blackcat_sgr.organization_name.isin(found_matches.organization_name.tolist()))]
    
    # Clean up
    found_matches = found_matches.drop(columns = ['organization_name']).rename(columns = {'fuzzy_agency':'organization_name'})
    
    return found_matches, still_need_matches

def merge_blackcat_sgr_airtable(merged_blackcat_sgr:pd.DataFrame, wrong_matches:list)->pd.DataFrame:
    """
    Merge Blackcat, SGR, and airtable information all together.
    This is the final dataframe.
    
    Args:
        merged_blackcat_sgr(df): df from merge_blackcat_sgr()
        wrong_matches (list): fuzzy matching isn't always correct, so this list
        deletes out the incorrect matches.
    """
    # Load in airtable
    airtable = airtable_service_type()
    airtable = clean_organization_names(airtable, "name")
    
    part1, part2 = fuzzy_match_airtable_bc(airtable, merged_blackcat_sgr,wrong_matches )
    
    # Concat
    blackcat_sgr = pd.concat([part1, part2], axis=0)
    
    # Manually map organization names
    blackcat_sgr.organization_name = blackcat_sgr.organization_name.replace(to_map)                
    
    m1 = pd.merge(
    blackcat_sgr,
    airtable,
    how="left",
    left_on="organization_name",
    right_on="name")
    
    m1.service_type = m1.service_type.fillna("no service info")
    m1 = m1.drop(columns=["name"])
    m1 = m1.fillna("NA")
    
    final_subset = [
    "funding_program",
    "organization_name",
    "first_name",
    "last_name",
    "email",
    "phone",
    "title",
    "service_type",]
    
    m1 = m1[final_subset]
    
    m1.service_type = m1.service_type.str.title()
    m1 = delete_repeated_element(m1, "funding_program")
    m1 = m1.drop_duplicates(subset = ['organization_name']).sort_values(["organization_name"]).reset_index(drop=True)
    
    return m1