"""
Compile cached results into dataframe.
"""
import pandas as pd
import pickle

import A4_geocode
import utils

# Parse the results dict and compile as pd.Series
def compile_results(results):
    # https://flexiple.com/check-if-key-exists-in-dictionary-python/
    # Get the value from key, but if it doesn't exist, put None
    # Some of the results are missing some component
    all_keys = [
        "x", "y",
        "addr:housenumber", "addr:street",
        "addr:city", "addr:state",
        "addr:country", "addr:postal"
    ]
    
    for i in all_keys:
        if i not in results.keys():
            results[i] = None
            
    longitude = results["x"]
    latitude = results["y"]
    house_number = results["addr:housenumber"]
    street = results["addr:street"]
    city = results["addr:state"]
    state = results["addr:state"]
    country = results["addr:country"]
    postal = results["addr:postal"]

    return pd.Series(
        [longitude, latitude, 
         house_number, street,
         city, state, country, postal], 
        index= ["longitude", "latitude", 
                "house_number", "street",
                "city", "state", "country", "postal"]
    )


if __name__ == "__main__":
    geocode_df = A4_geocode.prep_geocode_df()
    
    unique_uuid = list(geocode_df.sheet_uuid)
    
    full_results = pd.DataFrame()
    errors = []
    
    assembled_dict = {}
    
    for i in unique_uuid:
        try:
            # Grab cached result
            result = utils.open_request_json(
                i,
                DATA_PATH = utils.DATA_PATH,
                GCS_FILE_PATH = f"{utils.GCS_FILE_PATH}geocode_cache/"
            )
            
            # Compile JSON into pd.Series
            results_series = compile_results(result)
            assembled_dict[i] = results_series    
            
            # Transpose, so it shows up in columns
            # Add sheet_uuid, allow it to be merged back to df later
            results_df = (pd.DataFrame(results_series).T
                          .assign(sheet_uuid = i)
                         )
            full_results = pd.concat([full_results, results_df], 
                                     ignore_index=True)
        except:
            errors.append(i)
    
    
    with open(f"{utils.DATA_PATH}assembled_dict.pickle", "wb") as f:
        pickle.dump(assembled_dict, f)
        
    # Export results to GCS
    print(f"# geocoded results: {len(full_results)}")
    full_results.to_parquet(f"{utils.GCS_FILE_PATH}geocoder_results.parquet")
    
    # Export errors as pickle to double check
    with open(f"{utils.DATA_PATH}errors_compiling.pickle", "wb") as f:
        pickle.dump(errors, f)