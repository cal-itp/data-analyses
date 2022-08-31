#General packages
import numpy as np
import pandas as pd
import geopandas as gpd
from shared_utils import geography_utils, utils
from calitp import *

#For saving geojson to GCS
from calitp.storage import get_fs
fs = get_fs()
import os

'''
File Paths
'''
GCS_FILE_PATH = "gs://calitp-analytics-data/data-analyses/lctop/"
Caltrans_shape = "https://gis.data.ca.gov/datasets/0144574f750f4ccc88749004aca6eb0c_0.geojson?outSR=%7B%22latestWkid%22%3A3857%2C%22wkid%22%3A102100%7D"
FILE_NAME = "LCTOP_allyears.xlsx"

'''
Columns 
'''
boolean_cols = [
    "agency_service_area_has_a_dac",
    "does_project_benefit_an_ab_1550_dac",
    "status",
    "qualifying_1_2_mile_low_income_buffer_",
    "ab_1550_low_income_community__household",
    "does_project_benefit_an_ab_1550_dac",
]

drop_cols = [
        "count",
        "#",
        "column3",
        "column4",
        "column5",
        "other_state_policies,_plans,_or_initiatives",
        "describe_policies,_plans,_or_initiatives",
        "#2",
        "_d",
        "contact_name",
        "contact_phone_#",
        "contact_e_mail",
        "authorized_agent_name",
        "authorized_agent_title",
        "project_sub_type",
    ]

env_cols = [
    "vmt_reduction",
    "ghg_reduction__mtco2e_",
    "diesel_pm_reductions__lbs_",
    "nox_reductions__lbs_",
    "pm_2_5_reductions__lbs_",
    "reactive_organic_gas_reduction__lbs_",
    "fossil_fuel_use_reduction__transportation_",
    "fossil_fuel_use_reduction__energy_",
    "renewable_energy_generation__kwh_",
]

date_columns = ['qm_tool__date_', 'completion_date','start_date']
float_columns = ['ridership_increase','fossil_fuel_use_reduction__transportation_']

'''
Functions
'''
#Function to remove any dba:, special characters, typos, etc
def cleaning_agency_names(dataframe, col_of_int:str): 
    dataframe[col_of_int] = (dataframe[col_of_int]
                      .str.split("(")
                      .str[0]
                      .str.replace('[^A-Za-z\s]+', '')
                      .str.replace("Publlic","Public")
                      .str.replace("Regional Transit Authority","")
                      .str.replace("Agency","")
                      .str.replace("Commision","Commission")
                      .str.replace("Division","")
                      .str.split(",")
                      .str[0]
                      .str.strip()
                     )
    return dataframe

#Clean column titles
def cols_cleanup(df):
    df.columns = df.columns.str.replace("[_]", " ").str.title().str.strip()
    return df

#Save a gdf in geojson format to GCS
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

#Function for categorizing percentiles based on column of interest
#returns a new column with percentile 
def percentiles(df, col_of_interest: str): 
    #Get percentiles in objects for total vehicle.
    p75 = df[col_of_interest].quantile(0.75).astype(float)
    p25 = df[col_of_interest].quantile(0.25).astype(float)
    p50 = df[col_of_interest].quantile(0.50).astype(float)
    
    #actually categorize each value for percentile
    def percentile_categorize (row):
        if ((row[col_of_interest] > 0) and (row[col_of_interest] <= p25)):
            return "25 percentile"
        elif ((row[col_of_interest] > p25) and (row[col_of_interest]<= p75)):
            return "50th percentile"
        elif (row[col_of_interest]> p75):
               return "75th percentile"
        elif (row[col_of_interest] < 0):
               return "Increase" 
        else:
            return "Zero"
    df[f'{col_of_interest}_percentile'] = df.apply(lambda x: percentile_categorize(x), axis=1)
  
    return df

'''
Crosswalks
& Variables
'''
#Replacing agency names 
agency_crosswalk = {"Stanislaus County Public Works   Transit": "Stanislaus County Public Works Transit",
        "Stanislaus County Public Works  Transit": "Stanislaus County Public Works Transit",
        "Stanislaus County Public WorksTransit": "Stanislaus County Public Works Transit",
        "Victor ValleyTransit Authority": "Victor Valley Transit Authority",
        "YubaSutter Transit Authority": "Yuba Sutter Transit Authority",
        "YubaSutter Transit": "Yuba Sutter Transit Authority",
        "Plumas County Transportation  Commission": "Plumas County Transportation  Commission",
        "Modoc Transportation": "Modoc County Transportation Commission",
        "Los Angeles County Metoropolitan Transportation Authority": "Los Angeles County Metropolitan Transportation Authority",
        "Calaveras Transit": "Calaveras Transit Agnecy",
        "Arcata": "City of Arcata",
        "Arvin":'The City of Arvin',
        "Auburn": "City of Auburn",
        "El Dorado County Transit Authority":"El Dorado County Transit",
        "Golden Gate Bridge Highway  Transportation District": "Golden Gate Bridge Highway and Transportation District",
        "Plumas County Transportation  Commission":"Plumas County Transportation Commission"}

#Value to populate date columns that have missing values, so they show up in Tableau
missing_date = pd.to_datetime('2100-01-01')

'''
Import the original data
'''
def load_lctop():
    df1 =  to_snakecase(
    pd.read_excel(f"{GCS_FILE_PATH}{FILE_NAME}"))
    #Drop rows with nulls in the columns of importance
    df2 = df1.dropna(subset=['lead_agency', 'project_id#','project_name', 'distr_'])
    #Drop columns of disinterest 
    df2 = df2.drop(columns = drop_cols)
    return df2

def load_CT_district_shapes():
    geojson = gpd.read_file(f"{Caltrans_shape}").to_crs(epsg=4326)
    # Keep only the columns of interest
    geojson = geojson[["DISTRICT", "Shape_Length", "Shape_Area", "geometry"]]
    return geojson
    
'''
Clean up the data
'''
def clean_lctop():
    df1 = load_lctop() 
    #Clean up agency names: multiple spellings of the same agencies
    #First pass is to replace strange characters and typos.
    df2 = cleaning_agency_names(df1, 'lead_agency')
    #Second pass, use a crosswalk to map values
    df2['lead_agency'] = df2['lead_agency'].replace(agency_crosswalk)
    
    #There are columns that are encoded as yes/no and close/open.
    #but there are multiple spellings of the same value, clean them up
    for i in boolean_cols:
        df2[i] = (df2[i]
               .str.strip()
               .str.lower()
               .str.replace('close','closed')
               .str.replace('closedd','closed')
                        )
    #Strip the word 'county' from county column 
    df2['county'] = df2['county'].str.replace('County','')
    
    #Coerce columns that are the wrong type & fill in missing values
    for c in date_columns:
        df2[c] = df2[c].apply(pd.to_datetime, errors='coerce')
    for i in date_columns:
        df2[i] = (df2[i]
            .fillna(missing_date)
            .apply(pd.to_datetime)
                 )
    for c in float_columns:
        df2[c] = df2[c].apply(pd.to_numeric, errors = 'coerce')
    df2 = df2.fillna(df2.dtypes.replace({'float64': 0.0, 'object': 'None'}))

    #Save
    with pd.ExcelWriter(f"{GCS_FILE_PATH}LCTOP_cleaned.xlsx") as writer:
        df2.to_excel(writer, sheet_name="cleaned", index=False)    
    return df2


'''
Aggregating
Data for Metrics
'''
#Columns
sum_cols = [
    "funds_to_benefit_dac",
    "total_project_request_99314_+_99313",
    "total_project_cost",
    "vmt_reduction",
    "ghg_reduction__mtco2e_",
    "diesel_pm_reductions__lbs_",
    "nox_reductions__lbs_",
    "pm_2_5_reductions__lbs_",
    "reactive_organic_gas_reduction__lbs_",
    "fossil_fuel_use_reduction__transportation_",
    "ridership_increase",
    "fossil_fuel_use_reduction__energy_",
    "renewable_energy_generation__kwh_",
]
nunique_cols = ["project_id#", "lead_agency"]

metric_cols = [
    "Diesel Pm Reductions  Lbs",
    "Fossil Fuel Use Reduction  Energy",
    "Fossil Fuel Use Reduction  Transportation",
    "Funds To Benefit Dac",
    "Ghg Reduction  Mtco2E",
    "Nox Reductions  Lbs",
    "Pm 2 5 Reductions  Lbs",
    "Reactive Organic Gas Reduction  Lbs",
    "Renewable Energy Generation  Kwh",
    "Ridership Increase",
    "Total Project Cost",
    "Total Project Request 99314 + 99313",
    "Vmt Reduction",
    "# of Agencies",
    "# of Projects",
]

def district_fy_summary():
    original_df = clean_lctop()
    ct_districts = load_CT_district_shapes()
    #Aggregate
    summary = geography_utils.aggregate_by_geography(
    original_df,
    group_cols=[
        "funding_year",
        "distr_",
    ],
    nunique_cols=nunique_cols,
    sum_cols=sum_cols,)
    
    #Clean it up
    summary = (
    cols_cleanup(summary)
    .sort_values("Distr")
    .rename(columns={"Lead Agency": "# of Agencies", "Project Id#": "# of Projects"})
    )
    
    #Instead of having the raw values, change them into percentiles (results saved in new col)
    for i in metric_cols:
        summary = percentiles(summary, i)
        
    #Attach Caltrans District geometry 
    gdf1 = ct_districts.merge(
    summary, how="inner", left_on="DISTRICT", right_on="Distr")
    
    #Save
    geojson_gcs_export(
    gdf1,
    "gs://calitp-analytics-data/data-analyses/lctop/",
    "lctop_districts",) 
    return summary, gdf1