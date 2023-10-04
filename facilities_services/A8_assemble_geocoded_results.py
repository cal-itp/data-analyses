"""
Append geocoder and manually geocoded results.
"""
import geopandas as gpd
import intake
import pandas as pd

import utils as _utils
import A3_prep_for_geocode
from calitp_data_analysis import utils

catalog = intake.open_catalog("./*.yml")


if __name__ == "__main__":

    df = catalog.tier1_facilities_addresses.read()
    df = A3_prep_for_geocode.prep_for_geocoding(df)

    geocoder_df = utils.download_geoparquet(
        GCS_FILE_PATH = f"{_utils.GCS_FILE_PATH}",
        FILE_NAME = "geocoder_results"
    )
    
    manual_df = utils.download_geoparquet( 
        GCS_FILE_PATH = f"{_utils.GCS_FILE_PATH}", 
        FILE_NAME = "manually_geocoded_results"
    )
    
    
    # Merge #1: inner merge on sheet_uuid.
    # There are multiple addresses with different sheet_uuid, so
    # do a second merge to find these (can't merge using address because address_arcgis_clean 
    # doesn't match full_address inputed.
    address_cols = ["address_arcgis_clean", "geometry"]
    group_cols = ["full_address", "city", "zip_code"]

    m1 = pd.merge(
        df[group_cols + ["sheet_uuid"]], 
        geocoder_df[address_cols + ["sheet_uuid"]],
        on = "sheet_uuid",
        how = "inner",
        validate = "1:1",
    )
    
    # Merge #2: back to the same amt of obs in the for_geocoding df
    # Using full_address, can find the geometry for the same address, different sheet_uuid
    m2 = pd.merge(
        df, 
        m1[group_cols + address_cols],
        on = group_cols,
        how = "inner",
        validate = "m:1",
    )
    
    #  Now merge in manually geocoded results
    # First, subset to ones that need to be found in manual
    m3 = df[df.sheet_uuid.isin(manual_df.sheet_uuid)]
    m4 = pd.merge(
        m3, 
        manual_df[["sheet_uuid", "geometry"]],
        how = "inner",
        validate = "1:1"
    )
    
    # Concatenate
    final = (pd.concat([m2, m4], 
                       axis=0, ignore_index=True)
            )

    final = gpd.GeoDataFrame(final)
    
    # Check that the number of observations match
    print(f"# obs in original df: {len(df)}")
    print(f"# obs in final df: {len(final)}")
    
    # Export to GCS
    utils.geoparquet_gcs_export(
        final, 
        _utils.GCS_FILE_PATH, 
        "tier1_facilities_geocoded"
    )