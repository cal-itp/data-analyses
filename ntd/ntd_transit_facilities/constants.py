NTD_FACILITIES_INVENTORY_URL = "https://www.transit.dot.gov/sites/fta.dot.gov/files/2024-10/2023%20Facility%20Inventory.xlsx"
NTD_CRS = "EPSG: 4326"

# This should be accessible to the user
STREET_ADDRESS = "Street Address"
CITY = "City"
STATE = "State"
ZIP_CODE = "ZIP Code"
LATITUDE = "Latitude"
LONGITUDE = "Longitude"
AGENCY_NAME = "Agency Name"
FACILITY_TYPE = "Facility Type"
FACILITY_NAME = "Facility Name"
NTD_ADDRESS_FIELDS = (STREET_ADDRESS, CITY, STATE, ZIP_CODE)
DEFAULT_TOOLTIP_FIELDS = (AGENCY_NAME, FACILITY_NAME, FACILITY_TYPE, "Square Feet", "Number of Parking Spaces", "Section of a Larger Facility", "Administrative/Maintenance Facility Flag", "Notes", "Primary Mode Served", "Geometry Geocoded", "Geocode Result Address")

# Geocoding parameters
DOTENV_PATH = "_env"
GOOGLE_MAPS_API_KEY_ENV = "GOOGLE_MAPS_API_KEY"
GOOGLE_MAPS_ENGINE_NAME = "GoogleV3"
GEOPANDAS_ADDRESS_NAME = "address"
