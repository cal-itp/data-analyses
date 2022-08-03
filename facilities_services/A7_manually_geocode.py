"""
Manually geocode some locations

Some of the addresses are postmiles. 
Use open data: https://gisdata-caltrans.opendata.arcgis.com/datasets/c22341fec9c74c6b9488ee4da23dd967_0/
"""

import geopandas as gpd
import pandas as pd

import utils
import shared_utils

# Basic cleaning of SHN postmiles dataset
def clean_postmiles() -> gpd.GeoDataFrame:
    df = gpd.read_parquet(f"{utils.DATA_PATH}shn_postmiles.parquet")
    
    # Round to 2 decimal places
    # otherwise, floats are giving trouble
    df = df.assign(
        PM = df.PM.round(2)
    ).rename(columns = {"County": "county"})
    
    return df


# postmile adjustment
# sometimes rounding isn't the same, so it doesn't merge 
# correct these and they should merge on
ADDRESS_PM_DICT = {
    "HWY 88 PM 66.5": "HWY 88 PM 66.54",
    "HWY 49 PM 8.107": "HWY 49 PM 8.1",
    "HWY 395 PM 1152": "HWY 395 PM 11.54",
    "HWY 44 PM 339": "HWY 44 PM 33.9",
    "HWY 88 PM 134": "HWY 88 PM 13.4",
    "HWY 70 PM 707": "HWY 70 PM 70.7",
    "HWY 70 PM 552": "HWY 70 PM 55.2",
    "HWY 5 66.9": "HWY 5 PM 66.90",

}
    

# Subset and find those in manual geocoding list that would be found in postmiles df
def subset_manual_geocoding(df: gpd.GeoDataFrame) -> gpd.GeoDataFrame:
    df = df.assign(
        address = df.apply(
            lambda x: ADDRESS_PM_DICT[x.address] 
            if x.address in ADDRESS_PM_DICT.keys()
            else x.address, axis=1)
    )
    
    # These should be found in postmiles, with "Hwy X PM Y" pattern
    # Allow the ones with extra notes to stay, like "Hwy X PM Y ON NORTH WEED BLVD"
    df2 = df[(df.address.str.contains("PM")) & 
         (df.address.notna())][
        ["sheet_uuid", "address", "county"]]
    
    print(f"have postmiles: {len(df2)}")
    
    return df2


def parse_postmiles(df: gpd.GeoDataFrame) -> gpd.GeoDataFrame:
    df = df.assign(
        Route = (df.address.str.split(" PM ", expand=True)[0]
                        .str.replace("HWY", "").astype(int)
                       ),
        PM = (df.address.str.split(" PM ", expand=True)[1]),
    )
    
    df = df.assign(
        PM = df.PM.str.split(" ", expand=True)[0].astype(float).round(2)
    ) 
    
    return df


def find_centroid(df: pd.DataFrame) -> gpd.GeoDataFrame:
    """
    There are duplicates because there's a lat/lon for each direction (N/S, E/W)
    
    Take centroid vs keep one of the obs after explicitly sorting
    
    Either create new geometry or have a lat/lon that appears in postmiles df.
    Go with finding the centroid between N/S and E/W to be the centerline point.
    """
    # The merge was left_only, and is df, not gdf
    gdf = df.set_geometry("geometry")
    
    # Dissolve by sheet_uuid, then calculate centroid
    gdf2 = (gdf.dissolve(by="sheet_uuid").centroid
            .reset_index()
            .rename(columns = {0: "geometry"})
           )
    
    # Merge back in to df
    gdf3 = pd.merge(
        gdf2, 
        df[["sheet_uuid", "address", "county"]].drop_duplicates(),
        on = "sheet_uuid",
        how = "left",
        validate = "1:1",
    )
    
    return gdf3


def add_manual_results(df:pd.DataFrame)->gpd.GeoDataFrame: 
    ADDRESS_DICT2 = {
        # Where Pacific Crest Trail intersects with Hwy 80
        "BOREAL RIDGE ROAD & PACIFIC CREST TRAILWAY": {"longitude": -120.336147, 
                                                       "latitude": 39.342918},
        # A guess - these bldgs don't have any associated with on Google Maps
        # It's roughly 19 mi north of Ojai, off Hwy 33
        "19 MILES NORTH OF OJAI": {"longitude": -119.302167, 
                                   "latitude": 34.597436},
    } 

    # Find these in geocoder_results
    # Use full_address to match and create the lon/lat columns
    FULL_ADDRESS_DICT = {
        "Childcare center Oakland, CA": {"longitude": -122.264931, 
                                         "latitude": 37.810912}, 
        "Childcare center Los Angeles, CA": {"longitude": -118.243285, 
                                             "latitude": 34.051899},
        "Equipment shop Los Angeles, CA": {"longitude": -118.243285, 
                                           "latitude": 34.051899},
    }
    
    
    def add_lat_lon(row)-> pd.Series:
        # Return a pd.Series of a list object with [lon, lat] coordinates
        if row.full_address in FULL_ADDRESS_DICT.keys():
            longitude = FULL_ADDRESS_DICT[row.full_address]["longitude"]
            latitude = FULL_ADDRESS_DICT[row.full_address]["latitude"]
        
        elif row.address in ADDRESS_DICT2.keys():
            longitude = ADDRESS_DICT2[row.address]["longitude"]
            latitude = ADDRESS_DICT2[row.address]["latitude"]
        else:
            longitude = None
            latitude = None
            
        return pd.Series([longitude, latitude], 
                         index=["longitude", "latitude"])
    
    lat_lon = df.apply(add_lat_lon, axis=1)
    df2 = pd.concat([df, lat_lon], axis=1)
    
    gdf = shared_utils.geography_utils.create_point_geometry(
        df2,
        longitude_col="longitude",
        latitude_col = "latitude"
    ).drop(columns = ["longitude", "latitude"])
    

    return gdf


if __name__ == "__main__":
    df = pd.read_parquet(f"{utils.GCS_FILE_PATH}manual_geocoding.parquet")
    df2 = subset_manual_geocoding(df)
    df3 = parse_postmiles(df2)
    
    postmiles = clean_postmiles()

    df4 = pd.merge(
        df3, 
        postmiles,
        on = ["Route", "PM", "county"],
        how = "left",
        validate = "1:m",
        indicator=True
    )

    df5 = find_centroid(df4)
    
    # Add the results from ones manually figured out from Google Maps
    manual_ones = df[~df.sheet_uuid.isin(df5.sheet_uuid)]
    manual_ones2 = add_manual_results(manual_ones)
    
    gdf = pd.concat([df5, manual_ones2], axis=0, ignore_index=True)
    
    # Export to GCS
    shared_utils.utils.geoparquet_gcs_export(gdf, 
                                             utils.GCS_FILE_PATH, 
                                             "manually_geocoded_results"
                                            )
    