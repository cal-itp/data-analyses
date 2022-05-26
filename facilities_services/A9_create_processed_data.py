"""
More cleanup of geocoded data.

Aggregate to address (add up sqft, just keep 1 sheet_uuid).
Opt for sjoins on Caltrans districts and CA counties to fill
in missing info there (some observations came with it, others didn't)
"""
import geopandas as gpd
import intake
import pandas as pd

from calitp import to_snakecase

import utils
import shared_utils

catalog = intake.open_catalog("./*.yml")


def aggregate_to_address(gdf):
    # First, add up sqft for what's owned/leased and other
    gdf = gdf.assign(
        sqft = gdf[["sqft", "other"]].sum(axis=1)
    )

    drop_cols = ["full_address", "address_cleaned", 
                 "city", "zip_code", "zip_code2"]
    
    # Second, add up sqft for the same address 
    # Keep the sheet_uuid associated with largest sqft
    gdf2 = (gdf.sort_values(["address_arcgis_clean", "sqft"], 
                           ascending=[True, False])
           .assign(
               sqft = gdf.groupby("address_arcgis_clean").sqft.transform("sum"),
           ).drop_duplicates(subset=["address_arcgis_clean" ,"sqft"])
            .drop(columns = drop_cols)
          )
    
    return gdf2


# Join facilities (points) to some polygon geometry (county or district)
def sjoin_to_geography(df, geog_df):
    s1 = gpd.sjoin(
        df.to_crs(shared_utils.geography_utils.WGS84), 
        geog_df.to_crs(shared_utils.geography_utils.WGS84),
        how = "left",
        predicate = "intersects"
    ).drop(columns = "index_right")
    
    return s1
    
    
if __name__ == "__main__":

    # Aggregate facilities to addresses (keep 1 row per address)
    gdf = catalog.tier1_facilities_geocoded.read()
    gdf = aggregate_to_address(gdf)
    
    # CA counties
    counties = catalog.ca_counties.read()
    keep_cols = ["COUNTY_NAME", "COUNTY_ABBREV", 
                 "COUNTY_FIPS", "geometry"]
    counties = to_snakecase(counties[keep_cols])
    
    # Caltrans districts
    districts = catalog.caltrans_districts.read()
    keep_cols = ["DISTRICT", "geometry"]
    districts = to_snakecase(districts[keep_cols])
    
    
    # Spatial join to county
    gdf2 = sjoin_to_geography(
        gdf.drop(columns = ["county"]), counties)
    
    # Spatial join to district
    gdf3 = sjoin_to_geography(
        gdf2.rename(columns = {"district": "district_orig"}), districts)
    
    # Replace district if it wasn't able to join onto one
    # Or, keep the fact that HQ is designated district 59, even though it falls within Sac (D3)
    gdf4 = gdf3.assign(
        district = gdf3.apply(lambda x: x.district if (x.district_orig==x.district)
                                  else x.district_orig, axis=1)
    ).drop(columns = "district_orig")
    
    # Export to GCS
    shared_utils.utils.geoparquet_gcs_export(gdf4, 
                                             utils.GCS_FILE_PATH, 
                                             "tier1_facilities_processed"
                                            )