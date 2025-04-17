import pandas as pd
from datetime import datetime

MONTH_ABBR_YEAR_FMT = "%b%Y" #oct2023

def parse_monthyear_string(monthyear_string: str) -> tuple[int, str]:
    """
    Parse the NTD monthly key to the relevant
    components we need.
    oct2023 -> 2023 (year), October (month)
    """
    parsed_date = datetime.strptime(monthyear_string, MONTH_ABBR_YEAR_FMT)
    
    return parsed_date.year, parsed_date.strftime("%B")
    

def get_public_filename(monthyear_string: str) -> str:
    """
    Create the zipped filename we want in our public GCS.
    oct2023 -> 2023_October.zip
    """
    year, month = parse_monthyear_string(monthyear_string)
    return f"{year}_{month}.zip"


DATES = {
    # key is month of NTD data
    # value is the month_updated in the url we're downloading from (2 months ahead from key)
    # ex oct data is reported in december, so (oct20XX:"20XX-12")
    "oct2023": "2023-12",
    "nov2023": "2024-01",
    "dec2023": "2024-02",
    "jan2024": "2024-03",
    "feb2024": "2024-04",
    "mar2024": "2024-05",
    "apr2024": "2024-06",
    "may2024": "2024-07",
    "jun2024": "2024-08",
    "jul2024": "2024-09",
    "aug2024": "2024-10",
    "sep2024": "2024-11",
    "oct2024": "2024-12",
    "nov2024": "2025-01",
    "dec2024": "2025-02",
    "jan2025": "2025-03",
    "feb2025": "2025-04",
}