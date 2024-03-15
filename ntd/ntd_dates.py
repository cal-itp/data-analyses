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
    # value is the month_updated in the url we're downloading from
    "oct2023": "2023-12",
    "nov2023": "2024-01",
    "dec2023": "2024-02",
    "jan2024": "2024-03",
}