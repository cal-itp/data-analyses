"""
Validate the metadata dictionary input we supply
Certain fields are pre-filled, unlikely to change
If the key isn't there, then it'll be filled in with default
"""
import pandas as pd

from pydantic import BaseModel
from typing import Literal

class metadata_input(BaseModel):
    dataset_name: str
    publish_entity: str = "Data & Digital Services / California Integrated Travel Project"
    purpose: str
    abstract: str
    public_access: Literal["Public", "Restricted"] = "Public"
    creation_date: str
    beginning_date: str
    end_date: str
    place: str = "California"
    status: Literal["completed", "historicalArchive", "obsolete", 
                    "onGoing", "planned", "required", 
                    "underDevelopment"] = "completed"
    frequency: Literal["continual", "daily", "weekly",
                       "fortnightly", "monthly", "quarterly", 
                       "biannually", "annually", 
                       "asNeeded", "irregular", "notPlanned", 
                       "unknown"] = "monthly"
    theme_topic: str = "transportation"
    theme_keywords: dict    
    methodology: str
    data_dict_type: str = "XML"
    #data_dict_url: str
    readme: str = "https://github.com/cal-itp/data-analyses/blob/main/README.md"
    readme_desc: str = "This site allows you to access the code used to create this dataset and provides additional explanatory resources."
    contact_organization: str = "Caltrans"
    contact_person: str
    contact_email: str = "hello@calitp.org"
    horiz_accuracy: str = "4 meters"
    boilerplate_desc: str = "The data are made available to the public solely for informational purposes. Information provided in the Caltrans GIS Data Library is accurate to the best of our knowledge and is subject to change on a regular basis, without notice. While Caltrans makes every effort to provide useful and accurate information, we do not warrant the information Use Limitation - The data are made available to the public solely for informational purposes. Information provided in the Caltrans GIS Data Library is accurate to the best of our knowledge and is subject to change on a regular basis, without notice. While Caltrans makes every effort to provide useful and accurate information, we do not warrant the information to be authoritative, complete, factual, or timely. Information is provided on an 'as is' and an 'as available' basis. The Department of Transportation is not liable to any party for any cost or damages, including any direct, indirect, special, incidental, or consequential damages, arising out of or in connection with the access or use of, or the inability to access or use, the Site or any of the Materials or Services described herein."
    boilerplate_license: str = "License - Creative Commons 4.0 Attribution."
    

def fix_values_in_validated_dict(d: dict) -> dict:
    d["theme_keywords"] = fill_in_keyword_list(d["theme_keywords"])
    
    d["frequency"] = check_update_frequency(d["frequency"])
        
    d["revision_date"] = check_dates(d["revision_date"])
    d["creation_date"] = check_dates(d["creation_date"])
        
    return d


def fill_in_keyword_list(keyword_list: list = []) -> dict:
    if len(keyword_list) >= 5:
        filled_out_list =  {"ns1:CharacterString": ', '.join(keyword_list)}
        return filled_out_list
    else:
        return "Input minimum 5 keywords"
    
    
def check_update_frequency(string: str) -> str:
    """
    Validate the update frequency, be more lenient, and fix it   
    """
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