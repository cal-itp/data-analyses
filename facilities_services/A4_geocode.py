"""
Geocode addresses.

Cache results as JSON in GCS.
Compile and add to dataframe.
"""
import geocoder
import os
import pandas as pd
import pickle

import utils

def prep_geocode_df():
    df = pd.read_parquet(f"{utils.GCS_FILE_PATH}for_geocoding.parquet")
    
    # Now that we removed the stuff in parentheses, 
    # the same location comes up multiple times
    # just throw 1 into geocoder, merge it back in to full df later
    keep_cols = ["full_address", "city", "zip_code", "sheet_uuid"]

    # Keep sheet_uuid to cache json results and have an identifier for the name
    # but don't use it to merge it back in
    geocode_df = df[keep_cols].drop_duplicates(
        subset=["full_address", "city", "zip_code"])
    
    return geocode_df


def arcgis_geocode_address(row):
    input_address = row.full_address
    g = geocoder.arcgis(input_address)
    
    if g.ok is True:
        results = g.json
        utils.save_request_json(results, row.sheet_uuid,
                                DATA_PATH = utils.DATA_PATH,
                                GCS_FILE_PATH = f"{utils.GCS_FILE_PATH}arcgis_geocode/"
                               )
        
        print(f"Cached {row.sheet_uuid}")
        
        return row.sheet_uuid
    else:
        return None

    
    # https://stackoverflow.com/questions/26835477/pickle-load-variable-if-exists-or-create-and-save-it
# If pickle file is found, use it. Otherwise, create any empty pickle file
# to hold uuids that have cached results
def read_or_new_pickle(path, default_in_file):
    if os.path.isfile(path):
        with open(path, "rb") as f:
            try:
                return pickle.load(f)
            except Exception: # so many things could go wrong, can't be more specific.
                pass 
    with open(path, "wb") as f:
        pickle.dump(default_in_file, f)
        
    return default_in_file    


if __name__ == "__main__":
    geocode_df = prep_geocode_df()
     
    # Geocode and cache results
    # Assume it's going to take batches to compile
   
    # Returns empty list the first time
    PICKLE_FILE = "cached_results_uuid.pickle"
    have_results = read_or_new_pickle(f"{utils.DATA_PATH}{PICKLE_FILE}", [])
    
    unique_uuid = list(geocode_df.sheet_uuid)
    no_results_yet = list(set(unique_uuid).difference(set(have_results)))
    
    # New list to store results and add it to have_results
    new_results = []
    
    for i in no_results_yet:
        try:
            result_uuid = geocode_df[geocode_df.sheet_uuid == i].apply(
                lambda x: arcgis_geocode_address(x), axis=1)
            if result_uuid is not None:
                new_results.append(result_uuid)
        except:
            pass
    

    # Overwrite pickle and make sure no uuid is duplicated
    # our identifiers are string. if it's something else, like pd.Series, it's unusable
    # https://stackoverflow.com/questions/18352784/how-to-remove-all-of-certain-data-types-from-list
    new_results = [x for x in new_results if isinstance(x, str)]
    updated_results = list(set(have_results + new_results))
    
    with open(f"{utils.DATA_PATH}{PICKLE_FILE}", "wb") as f:
        pickle.dump(updated_results, f)
        