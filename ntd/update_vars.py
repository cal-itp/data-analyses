import pandas as pd

import ntd_dates

GCS_FILE_PATH = "gs://calitp-analytics-data/data-analyses/ntd/"

current_month = "dec2023"
YEAR, MONTH = ntd_dates.parse_monthyear_string(current_month)
PUBLIC_FILENAME = ntd_dates.get_public_filename(current_month)
MONTH_CREATED = ntd_dates.DATES[current_month]

# Check this url each month
# https://www.transit.dot.gov/ntd/data-product/monthly-module-adjusted-data-release
# Depending on if they fixed the Excel, there may be an additional suffix
suffix = ""
FULL_URL = (
    "https://www.transit.dot.gov/sites/fta.dot.gov/files/"
    f"{MONTH_CREATED}/{MONTH}%20{YEAR}%20"
    "Complete%20Monthly%20Ridership%20%28with%20"
    f"adjustments%20and%20estimates%29{suffix}.xlsx"
)   

NTD_MODES = {
    "CB": "Commuter Bus",
    "CC": "Cable Car",
    "CR": "Commuter Rail",
    "DR": "Demand Response",
    "FB": "Ferryboats",
    "HR": "Heavy Rail",
    "LR": "Light Rail",
    "MB": "Bus",
    "MG": "Monorail / Automated Guideway",
    "RB": "Bus Rapid Transit",
    "SR": "Streetcar",
    "TB": "Trolleybus",
    "VP": "Vanpool",
    "YR": "Hybrid Rail",
}

NTD_TOS = {
    "DO": "Directly Operated",
    "PT": "Purchased Transportation",
    "TN": "Purchased Transportation - Transportation Network Company",
    "TX": "Purchased Transportation - Taxi"
}