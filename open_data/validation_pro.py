import pandas as pd

from shared_utils import rt_dates

def fill_in_keyword_list(keyword_list: list = []) -> list[dict]:
    if len(keyword_list) >= 5:
        filled_out_list =  [{"ns1:CharacterString": i} for i in keyword_list]
        return filled_out_list
    else:
        return "Input minimum 5 keywords"
    
# Validate the update frequency, be more lenient, and fix it   
def check_update_frequency(string):
    UPDATE_FREQUENCY = [
        "continual", "daily", "weekly",
        "fortnightly", "monthly", "quarterly", 
        "biannually", "annually", 
        "asNeeded", "irregular", "notPlanned", "unknown"
    ]
    
    if string in UPDATE_FREQUENCY:
        return string
    else:
        print(f"Valid update frequency values: {UPDATE_FREQUENCY}")    

        
def check_dates(string: str)-> str:
    """
    date1 = '2021-06-01'
    date2 = '1/1/21'
    date3 = '03-05-2021'
    date4 = '04-15-22'
    date5 = '20200830'
    """
    return pd.to_datetime(string).date().isoformat()            