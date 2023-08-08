import pandas as pd
import _portfolio_utils

# Geography
from shared_utils import geography_utils
import geopandas as gpd

# Format currency
from babel.numbers import format_currency

GCS_FILE_PATH = "gs://calitp-analytics-data/data-analyses/project_prioritization/"

# Save Geojson
from calitp_data_analysis import *
from calitp_data_analysis import get_fs
fs = get_fs()
import os

# Function to clean agency/organization names
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
        .str.title()
        .str.replace("Trasit", "Transit")
        .str.strip()  # strip again after getting rid of certain things
    )
    return df


# Natalie's function
def align_funding_numbers(df, list_of_cols):
    """
    Round dollar amounts by thousands.
    """
    for col in list_of_cols:
        df[col] = df[col] / 1000

    return df

# Export a GDF as a geojson to GCS
def geojson_gcs_export(gdf, GCS_FILE_PATH, FILE_NAME):
    """
    Save geodataframe as parquet locally,
    then move to GCS bucket and delete local file.

    gdf: geopandas.GeoDataFrame
    GCS_FILE_PATH: str. Ex: gs://calitp-analytics-data/data-analyses/my-folder/
    FILE_NAME: str. Filename.
    """
    gdf.to_file(f"./{FILE_NAME}.geojson", driver="GeoJSON")
    fs.put(f"./{FILE_NAME}.geojson", f"{GCS_FILE_PATH}{FILE_NAME}.geojson")
    os.remove(f"./{FILE_NAME}.geojson")

    
# Categorize a project by percentiles of whatever column you want
def project_size_rating(dataframe, original_column: str, new_column: str):
    """Rate a project by percentiles and returning percentiles for any column
    
    Args:
        dataframe
        original_column (str): column to create the metric off of
        new_column (str): new column to hold results
    Returns:
        the dataframe with the new column with the categorization.
    """
    # Get percentiles 
    p75 = dataframe[original_column].quantile(0.75).astype(float)
    p25 = dataframe[original_column].quantile(0.25).astype(float)
    p50 = dataframe[original_column].quantile(0.50).astype(float)

    # Function for fleet size
    def project_size(row):
        if (row[original_column] > 0) and (row[original_column] <= p25):
            return "25th percentile"
        elif (row[original_column] > p25) and (row[original_column] <= p75):
            return "50th percentile"
        elif row[original_column] > p75:
            return "75th percentile"
        else:
            return "No Info"

    dataframe[new_column] = dataframe.apply(lambda x: project_size(x), axis=1)

    return dataframe

# Grab value counts and turn it into a dataframe
def value_counts_df(df, col_of_interest):
    df = (
    df[col_of_interest]
    .value_counts()
    .to_frame()
    .reset_index()
    )
    return df 

# Strip snakecase when dataframe is finalized
def clean_up_columns(df):
    df.columns = df.columns.str.replace("_", " ").str.strip().str.title()
    return df

# Official Caltrans District names
district_dictionary = {
    7: "07 - Los Angeles",
    4: "04 - Oakland",
    2: "02 - Redding",
    9: "09 - Bishop",
    10: "10 - Stockton",
    11: "11 - San Diego",
    3: "03 - Marysville",
    12: "12 - Irvine",
    8: "08 - San Bernardino",
    5: "05 - San Luis Obispo",
    6: "06 - Fresno",
    1: "01 - Eureka",
    75: "75 - HQ",
    74: "74 - HQ",
    0: "None",
}

"""
Geography Functions
"""
Caltrans_shape = "https://gis.data.ca.gov/datasets/0144574f750f4ccc88749004aca6eb0c_0.geojson?outSR=%7B%22latestWkid%22%3A3857%2C%22wkid%22%3A102100%7D"

def create_caltrans_map(df):
    
    # Load in Caltrans shape
    ct_geojson = gpd.read_file(f"{Caltrans_shape}").to_crs(epsg=4326)
   
    # Inner merge 
    districts_gdf = ct_geojson.merge(
    df, how="inner", left_on="DISTRICT", right_on="district_number")
    
    return districts_gdf

# Merge a dataframe with county geography to return a gdf
ca_gdf = "https://opendata.arcgis.com/datasets/8713ced9b78a4abb97dc130a691a8695_0.geojson"

def create_county_map(df, left_df_merge_col:str, 
                      right_df_merge_col:str):
    
    # Load in Caltrans shape
    county_geojson = gpd.read_file(f"{ca_gdf}").to_crs(epsg=4326)
    
    # Keep only the columns we want
    county_geojson = county_geojson[['COUNTY_NAME', 'COUNTY_ABBREV','geometry']]
    
    # Replace abbreviations to Non SHOPP
    county_geojson["COUNTY_ABBREV"] = county_geojson["COUNTY_ABBREV"].replace(
        {
        "LOS": "LA",
        "DEL": "DN",
        "SFO": "SF",
        "SMT": "SM",
        "MNT": "MON",
        "SDG": "SD",
        "CON": "CC",
        "SCZ": "SCR",
        "SJQ": "SJ",
        "SBA": "SB",
    }
    )
    
    # Inner merge 
    county_df = county_geojson.merge(
    df, how="inner", left_on=left_df_merge_col, right_on=right_df_merge_col)

    return county_df

# Create Tableau Map with district information
def tableau_district_map(df, col_wanted):
    """
    df: original dataframe to summarize
    col_wanted: to column to groupby
    """
    df_districts = _portfolio_utils.summarize_districts(df, col_wanted)

    # Reverse the dictionary with district names because
    # final DF has the full names like 04-Bay Area and I only need 4
    # https://stackoverflow.com/questions/483666/reverse-invert-a-dictionary-mapping
    inv_district_map = {v: k for k, v in district_dictionary.items()}

    # Remap districts to just the number
    df_districts["district_number"] = df_districts["District"]

    # Map the inverse
    df_districts["district_number"] = df_districts["district_number"].replace(
        inv_district_map
    )

    # Join df above with Caltrans shape
    districts_gdf = create_caltrans_map(df_districts)

    # Drop cols
    unwanted_cols = [
        "DISTRICT",
        "OBJECTID",
        "Region",
        "district_number",
        "Shape_Length",
        "Shape_Area",
    ]
    districts_gdf = districts_gdf.drop(columns=unwanted_cols)

    # Save to GCS
    geojson_gcs_export(
        districts_gdf,
        GCS_FILE_PATH,
        "districts",
    )
    
    print('Saved to GCS') 
    
    return districts_gdf

