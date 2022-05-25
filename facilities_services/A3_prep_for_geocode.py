"""
Prep data for geocoding.

Clean up addresses.
Also spit out csv to do manual geocodes
where addresses can't be constructed.
"""
import intake
import pandas as pd
import re

import utils

catalog = intake.open_catalog("./*.yml")

def clean_up_addresses(row):
    if row.address is not None:
        address_string = row.address

        # Replace everything within parentheses
        address_cleaned = re.sub(r'\([^)]*\)', '', address_string)

        # They prefix some locations with these
        remove_me = ["TMC, ", "Lab, ", "Space adjustment: " ]

        for word in remove_me:
            address_cleaned = address_cleaned.replace(word, "")
            
        # Strip extra leading or trailing spaces
        address_cleaned = address_cleaned.strip()
        
        return address_cleaned
    else:
        return None 
    
def assemble_full_address(row):
    if (row.address_cleaned is not None) and (row.city is not None):
        
        address_city = row.address_cleaned + " " + row.city
        address_city = address_city.replace("None", "")
        address_with_zip = address_city + ", CA " + str(row.zip_code)
        address_with_zip = address_with_zip.replace("None", "")
        
        if row.category=="office":
            full_address = address_city + ", CA"
        elif ((row.category=="maintenance") and (row.zip_code2 is None)):
            full_address = address_with_zip
        elif (row.category=="maintenance") and (row.zip_code2 is not None):
            full_address = address_with_zip + "-" + row.zip_code2
        elif (row.category=="equipment") or (row.category=="labs"):
            full_address = address_with_zip
    else:
        full_address = "Incomplete"
    
    full_address = full_address.replace("-None", "").replace("None", "")

    return full_address


def prep_for_geocoding(df):
    df = df.assign(
        address_cleaned = df.apply(lambda x: clean_up_addresses(x), axis=1)
    )
    df = df.assign(
        full_address = df.apply(lambda x: assemble_full_address(x), axis=1)
    )
    
    return df


if __name__ == "__main__":
    df = catalog.tier1_facilities_addresses.read()
    
    df = prep_for_geocoding(df)
    
    # Do these manually
    # if there are no digits, in addition to those we know are incomplete
    manual_geocoding = df[~(df.full_address.str.contains(r"[0-9]"))]
    
    keep_cols = ["sheet_uuid", 
                 "address", "address_cleaned", "city", "zip_code", "zip_code2",
                 "full_address", 
                 "county", "facility_name"
                ]
    
    manual_geocoding[keep_cols].to_csv(f"{utils.GCS_FILE_PATH}manual_geocoding.csv", index=False)

    # These can go into a geocoder
    for_geocoding = df[(df.full_address.str.contains(f"[0-9]"))] 
    for_geocoding.to_parquet(f"{utils.GCS_FILE_PATH}for_geocoding.parquet")
    
