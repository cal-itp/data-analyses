import pandas as pd

import ntd_dates

GCS_FILE_PATH = "gs://calitp-analytics-data/data-analyses/ntd/"

current_month = "nov2023"
YEAR, MONTH = ntd_dates.parse_monthyear_string(current_month)
PUBLIC_FILENAME = ntd_dates.get_public_filename(current_month)
MONTH_CREATED = ntd_dates.DATES[current_month]

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