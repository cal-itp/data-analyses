"""
Save hwy related outputs in GCS for sharing.

There are 2 source datasets that need to be filtered for each district.
* `transit-on-shn` uses 5 mile segment
* `transit-desert` uses 10 mile segment.
* `better_bus_utils` adds on the `caltrans_district` column...use `District` or `caltrans_district`?
* since hwy_segment_id appears in both, just rename it to 5 mile or 10 mile id so that it's clear for exports
"""
import geopandas as gpd
import pandas as pd

from bus_service_utils import better_bus_utils
from calitp_data_analysis import utils

BUS_SERVICE_GCS = "gs://calitp-analytics-data/data-analyses/bus_service_increase/"
RECS_GCS = "gs://calitp-analytics-data/data-analyses/one_hundred_recs/"


def prep_transit_on_shn_five_mile_hwy_segments(
    analysis_date: str) -> gpd.GeoDataFrame:
    """
    Last minute wrangling to get the 10 mile segment df.
    Rename and drop columns that would be confusing for users.
    """
    gdf = better_bus_utils.get_sorted_highway_corridors()
    
    gdf = (gdf.assign(date = analysis_date)
           .rename(columns = {"hwy_segment_id": "hwy_segment_id_5mi"})
    )
    
    reordered_cols = [c for c in gdf.columns if c != "geometry"]
    gdf = gdf.reindex(columns = reordered_cols + ["geometry"])
    
    return gdf


def prep_transit_deserts_uncompetitive_ten_mile_hwy_segments(
    analysis_date: str) -> gpd.GeoDataFrame:
    """
    Last minute wrangling to get the 10 mile segment df.
    Rename and drop columns that would be confusing for users.
    """
    
    gdf = gpd.read_parquet(
        f"{BUS_SERVICE_GCS}highway_segments_by_competitive_category_{analysis_date}.parquet")
       
    gdf = (gdf.assign(date = analysis_date)
           .rename(columns = {
               "hwy_segment_id": "hwy_segment_id_10mi",
                "pct_uncompetitive": "pct_transit_routes_uncompetitive"})
           .drop(columns = ["_merge"]) 
           # _merge is left_only when there is no transit on that SHN segment
           # drop because users won't know how to use it
           # leave NaNs in pct_transit_routes_uncompetitive because having no transit
           # means you can't calculate how many routes are uncompetitive
          )
    
    reordered_cols = [c for c in gdf.columns if c != "geometry"]
    gdf = gdf.reindex(columns = reordered_cols + ["geometry"])
    
    return gdf


def export_by_district(analysis_date: str):
    gdf1 = prep_transit_on_shn_five_mile_hwy_segments(analysis_date)
    gdf2 = prep_transit_deserts_uncompetitive_ten_mile_hwy_segments(analysis_date)
    
    district_col = "District"
    districts = gdf1[gdf1[district_col].notna()][district_col].unique().tolist()
    
    for d in districts:
        print(f"District: {d}")
        district1 = gdf1[gdf1[district_col] == d].reset_index(drop=True)
        district2 = gdf2[gdf2[district_col] == d].reset_index(drop=True)
        
        utils.geojson_gcs_export(
            district1, 
            RECS_GCS,
            f"d{d}_hwy_segments_5mile",
            geojson_type = "geojson"
        )
    
        utils.geojson_gcs_export(
            district2,
            RECS_GCS,
            f"d{d}_hwy_segments_10mile",
            geojson_type = "geojson"
        )
        
        print(f"Exported district {d} geojsons")

        
if __name__ == "__main__":

    # pin this date, since it was dropped earlier...save as string
    # to not get datetime issues
    ANALYSIS_DATE = "2022-05-04"
    export_by_district(ANALYSIS_DATE)