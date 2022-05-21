"""
Compile cached results into dataframe.
"""
import pandas as pd
import pickle

import A4_geocode
import utils

# Parse the results dict and compile as pd.Series
def compile_results(results):
            
    address_arcgis_clean = results["address"]
    address_input = results["raw"]["name"]
    longitude = results["lng"]
    latitude = results["lat"]

    return pd.Series(
        [address_arcgis_clean, address_input, 
         longitude, latitude], 
        index= ["address_arcgis_clean", "address_input", 
                "longitude", "latitude"]
    )


if __name__ == "__main__":
    geocode_df = A4_geocode.prep_geocode_df()

    unique_uuid = list(geocode_df.sheet_uuid)
    
    full_results = pd.DataFrame()
        
    for i in unique_uuid:
        # Grab cached result
        result = utils.open_request_json(
            i,
            DATA_PATH = utils.DATA_PATH,
            GCS_FILE_PATH = f"{utils.GCS_FILE_PATH}arcgis_geocode/"
        )

        # Compile JSON into pd.Series
        results_series = compile_results(result)

        # Transpose, so it shows up in columns
        # Add sheet_uuid, allow it to be merged back to df later
        results_df = (pd.DataFrame(results_series).T
                      .assign(sheet_uuid = i)
                     )
        full_results = pd.concat([full_results, results_df], 
                                 ignore_index=True)
    
        
    # Export results to GCS
    print(f"# geocoded results: {len(full_results)}")
    full_results.to_parquet(f"{utils.GCS_FILE_PATH}geocoder_results.parquet")
