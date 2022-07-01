"""
Validation functions.

Functions here are called in `metadata_update`.

Dictionary inputs can be more lenient,
functions here will fix it into correct format if it's 
off by a little.

Once fixed, the dictionary is checked again with pydantic 
in `metadata_update`.
"""
import pandas as pd

# Function to put in list of keywords (MINIMUM 5 needed)
def fill_in_keyword_list(topic='transportation', keyword_list = []):
    if len(keyword_list) >= 5:
        filled_out_list = [
            {'themekt': 'ISO 19115 Topic Categories',
             'themekey': topic},
             {'themekt': 'None',
              'themekey': keyword_list
             }
        ]

        return filled_out_list
    else:
        return "Input minimum 5 keywords"

    
# Validate the data dict format (CSV or XML, for our case)
# But be more lenient and take 'csv', 'xml' and fix it
def check_data_dict_format(string):
    DATA_DICT_FORMAT = ["CSV", "XML"]

    if string.upper() in DATA_DICT_FORMAT:
        return string.upper()
    elif string in DATA_DICT_DICT_FORMAT:
        return string
    else: 
        print(f"Valid data dictionary formats: {DATA_DICT_FORMAT}.")  
    
    
# Validate the update frequency, be more lenient, and fix it     
def check_update_frequency(string):
    UPDATE_FREQUENCY = [
        "Continual", "Daily", "Weekly",
        "Fortnightly", "Monthly", "Quarterly", 
        "Biannually", "Annually", 
        "As Needed", "Irregular", "Not Planned", "Unknown"
    ]
    
    if string.title() in UPDATE_FREQUENCY:
        return string.title()
    elif string in UPDATE_FREQUENCY:
        return string
    else:
        print(f"Valid update frequency values: {UPDATE_FREQUENCY}")    

        
def check_dates(string):
    """
    date1 = '2021-06-01'
    date2 = '1/1/21'
    date3 = '03-05-2021'
    date4 = '04-15-22'
    date5 = '20200830'
    """
    date = pd.to_datetime(string).date()
    
    # Always want month and day to be 2 digit string
    # date5 is the case that is hardest to parse correctly, and pd.to_datetime() does it, but datetime.datetime doesn't do it correctly
    # https://stackoverflow.com/questions/3505831/in-python-how-do-i-convert-a-single-digit-number-into-a-double-digits-string
    def format_month_day(value):
        return str(value).zfill(2)

    valid_date = (str(date.year) + 
                  format_month_day(date.month) + 
                  format_month_day(date.day)
                 )
    
    return valid_date
    
        
# First time metadata is generated off of template, it holds '-999' as value
# Subsequent updates, pull it, and add 1
def check_edition_add_one(metadata):
    input_edition = metadata["idinfo"]["citation"]["citeinfo"]["edition"]
    
    if input_edition == '-999':
        new_edition = str(1)
    else:
        new_edition = str(int(input_edition) + 1)
    
    return new_edition

    
def check_horiz_accuracy(string):
    # Must be in decimal degrees
    if " decimal degrees" not in string:
        print("Convert horizontal accuracy to decimal degrees")
    if "0." not in string:
        print("Use leading zero before decimal degrees")
    else:
        return string