import _harmonization_utils
import geopandas as gpd
import numpy as np
import pandas as pd
from calitp_data_analysis import utils
from calitp_data_analysis.sql import to_snakecase

def sb1_basic_cleaning(
    gdf: gpd.GeoDataFrame, project_name_col: str, agency_col: str
) -> gpd.GeoDataFrame:
    """
    Do some basic cleaning before joining
    """
    # Some project names contain the year. Remove anything after 20..
    gdf[project_name_col] = gdf[project_name_col].str.split("20").str[0]

    # Get rid of | in object cols
    # https://stackoverflow.com/questions/68152902/extracting-only-object-type-columns-in-a-separate-list-from-a-data-frame-in-pand
    string_cols = gdf.select_dtypes(include=["object"]).columns.to_list()
    try:
        for i in string_cols:
            gdf[i] = (
                gdf[i]
                .str.replace("|", "")
                .str.title()
                .str.replace("[^\w\s]", "")
                .str.strip()
            )
            gdf[i] = gdf[i].fillna("None")
    except:
        pass

    # Project agency always says "Los Angeles submitted by county"
    # or "Fremont submitted by city." Remove submitted by.
    gdf[agency_col] = gdf[agency_col].str.replace("Submitted By", " ")

    return gdf

def fill_in_missing_info(gdf: gpd.GeoDataFrame) -> gpd.GeoDataFrame:
    """
    The same project (same projectid, projname,
    description) can have its info scattered among 1+ rows.
    Fill in missing values for the rest of the columns.
    """
    # Sort to make sure the same projects are all together
    group_cols = ["projectid", "projname", "description", "projstatus"]
    df = gdf.sort_values(group_cols)
    
    money_cols = ["totalcosts", "cost", "costfull"]

    etc_cols = [
        "projagency",
        "constyear",
        "routes",
        "multiprogfunded",
        "projstatus",
        "appagencyname",
        "impagencyname",
    ]
    # Ascending false because I want the largest value first
    for i in money_cols:
        df = df.sort_values(i, ascending=False)
        df[i] = df.groupby(group_cols)[i].ffill()
    
    # Descending false because I want populated cols first
    for i in etc_cols:
        df = df.replace(regex="None", value=np.nan)
        df = df.sort_values(i, ascending=True)
        df[i] = df.groupby(group_cols)[i].ffill()

    return df

def load_sb1() -> gpd.GeoDataFrame:
    """
    Load & clean all the projects on the SB1 map from the Feature Server.

    https://odpsvcs.dot.ca.gov/arcgis/rest/services/SB1/SB1_ProjectData/FeatureServer
    """
    sb1_pt1 = "https://odpsvcs.dot.ca.gov/arcgis/rest/services/SB1/SB1_ProjectData/FeatureServer/"
    sb1_pt2 = "/query?where=1%3D1&objectIds=&time=&geometry=&geometryType=esriGeometryEnvelope&inSR=&spatialRel=esriSpatialRelIntersects&distance=&units=esriSRUnit_Foot&relationParam=&outFields=*+&returnGeometry=true&maxAllowableOffset=&geometryPrecision=&outSR=&gdbVersion=&historicMoment=&returnDistinctValues=false&returnIdsOnly=false&returnCountOnly=false&returnExtentOnly=false&orderByFields=&groupByFieldsForStatistics=&outStatistics=&returnZ=false&returnM=false&multipatchOption=&resultOffset=&resultRecordCount=&returnTrueCurves=false&sqlFormat=none&f=geojson"

    full_gdf = pd.DataFrame()
    for i in list(map(str, [*range(0, 16)])):
        df = to_snakecase(gpd.read_file(f"{sb1_pt1}{i}{sb1_pt2}"))
        full_gdf = pd.concat([full_gdf, df], axis=0)

    # Basic cleaning
    full_gdf = sb1_basic_cleaning(
        full_gdf,
        "projname",
        "projagency",
    )

    # Find missing geo
    missing_geo = full_gdf[(full_gdf.geometry.is_empty)]
    print(f"{len(missing_geo)} rows are mising geometry")

    # Find invalid geo
    invalid_geo = full_gdf[~full_gdf.geometry.is_valid].reset_index(drop=True)
    print(f"{len(invalid_geo)} rows contain invalid geography")

    # Filter out completed projects which aren't of interest
    full_gdf = full_gdf.loc[full_gdf.projstatus != "Completed"].reset_index(drop=True)

    # Cols to keep
    keep = [
        "projectid",
        "projname",
        "projcatcode",
        "projcategory",
        "projprogcode",
        "projprogram",
        "multiprogfunded",
        "projstatus",
        "description",
        "cost",
        "assemblydistrict",
        "senatedistrict",
        "assemblycode",
        "senatecode",
        "countyname",
        "cityname",
        "countycode",
        "citycode",
        "appagencyname",
        "impagencyname",
        "geometry",
        "totalcosts",
        "routes",
        "constyear",
        "costfull",
        "projagency",
    ]
    
    # Only keep cols of interest
    full_gdf = full_gdf[keep]
    
    # Fill in missing info for the same project
    full_gdf = fill_in_missing_info(full_gdf)
    
    return full_gdf

def one_row_one_project(gdf: gpd.GeoDataFrame) -> pd.DataFrame:
    """
    SB1 dataset contains many duplicates. While some projects
    can have 1+ row if construction takes place in more than 1
    location, the project table dictates that one project can only have
    one corresponding row and this function executes this.
    """
    # Count number of nans
    gdf["number_of_nans"] = gdf.isnull().sum(axis=1)

    # We want the row with the least number of nans to be kept.
    gdf = gdf.sort_values(["number_of_nans"], ascending=True).reset_index(drop=True)
    
    # Drop the geometry column here. 
    dup_cols = [
        "projectid",
        "projname",
        "projcatcode",
        "projcategory",
        "projprogcode",
        "projprogram",
        "multiprogfunded",
        "projstatus",
        "description",
        "cost",
        "assemblydistrict",
        "senatedistrict",
        "assemblycode",
        "senatecode",
        "countyname",
        "countycode",
        "appagencyname",
        "impagencyname",
        "totalcosts",
        "routes",
        "constyear",
        "costfull",
        "projagency",
    ]

    gdf = (
        gdf[dup_cols]
        .drop_duplicates(subset=dup_cols)
        .reset_index(drop=True)
    )
    
    return gdf

