GCS_FILE_PATH = "gs://calitp-analytics-data/data-analyses/ntd/"

YEAR = "2023"
MONTH = "October"
MONTH_CREATED = "2023-12"
PUBLIC_FILENAME = f"{YEAR}_{MONTH}.zip"

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