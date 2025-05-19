import pathlib
from calitp_data_analysis.geography_utils import WGS84

# Using a GCS version of the NTD url, since currently the official NTD download link will not download using requests.get or pandas.read_excel
NTD_FACILITIES_INVENTORY_PARENT = "gs://calitp-analytics-data/data-analyses/ntd/ntd_transit_facilities/"
ORIGINAL_NTD_FACILITIES_INVENTORY_URI = f"{NTD_FACILITIES_INVENTORY_PARENT}2023_facilities_inventory_unprocessed.xlsx"
PROCESSED_GEOJSON_URI = f"{NTD_FACILITIES_INVENTORY_PARENT}2023_facilities_inventory_processed.geojson"
PROCESSED_SHAPEFILE_URI = f"{NTD_FACILITIES_INVENTORY_PARENT}2023_facilities_inventory_processed.zip"
NTD_CRS = WGS84

# Define NTD Field Names
STREET_ADDRESS = "Street Address"
CITY = "City"
STATE = "State"
ZIP_CODE = "ZIP Code"
LATITUDE = "Latitude"
LONGITUDE = "Longitude"
AGENCY_NAME = "Agency Name"
FACILITY_TYPE = "Facility Type"
FACILITY_NAME = "Facility Name"
SQUARE_FEET = "Square Feet"
NUMBER_PARKING_SPACES = "Number of Parking Spaces"
LARGER_FACILITY_FLAG = "Section of a Larger Facility"
ADMINISTRATIVE_MAINTENANCE_FLAG = "Administrative/Maintenance Facility Flag"
NOTES = "Notes"
PRIMARY_MODE_SERVED = "Primary Mode Served"

# Added field names
GEOMETRY_GEOCODED = "Geometry Geocoded"
GEOCODE_RESULT_ADDRESS = "Geocode Result Address"

NTD_ADDRESS_FIELDS = (STREET_ADDRESS, CITY, STATE, ZIP_CODE)
DEFAULT_TOOLTIP_FIELDS = (
    AGENCY_NAME, 
    FACILITY_NAME, 
    FACILITY_TYPE, 
    SQUARE_FEET, 
    NUMBER_PARKING_SPACES, 
    LARGER_FACILITY_FLAG, 
    ADMINISTRATIVE_MAINTENANCE_FLAG, 
    NOTES, 
    PRIMARY_MODE_SERVED, 
    GEOMETRY_GEOCODED, 
    GEOCODE_RESULT_ADDRESS,
)
SHAPEFILE_MAP = {
    AGENCY_NAME: "AGENCY",
    FACILITY_NAME: "FAC_NAME",
    FACILITY_TYPE: "FAC_TYPE",
    SQUARE_FEET: "SQUARE_FT",
    NUMBER_PARKING_SPACES: "NUM_SPACES",
    LARGER_FACILITY_FLAG: "SECT_FLG",
    ADMINISTRATIVE_MAINTENANCE_FLAG: "ADM_MT_FLG",
    NOTES: "NOTES",
    PRIMARY_MODE_SERVED: "MAIN_MODE",
    GEOMETRY_GEOCODED: "GEOCODED",
    GEOCODE_RESULT_ADDRESS: "ADDRESS",
}

# Geocoding parameters
DOTENV_PATH = "_env"
GOOGLE_MAPS_API_KEY_ENV = "GOOGLE_MAPS_API_KEY"
GOOGLE_MAPS_ENGINE_NAME = "GoogleV3"
GEOPANDAS_ADDRESS_NAME = "address"
