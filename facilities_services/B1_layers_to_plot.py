"""
"""

import geopandas as gpd
import intake
import pandas as pd

import utils
from shared_utils import geography_utils

catalog = intake.open_catalog("./*.yml")


def subset_hqta():
    hqta = catalog.hqta_shapes.read()

    exclude_me = [
        "major_stop_ferry", "major_stop_rail",
    ]

    hqta = hqta[~hqta.hqta_type.isin(exclude_me)]

    hqta_cols = [
        'calitp_itp_id_primary', 'agency_name_primary', 'hqta_type',
        'calitp_itp_id_secondary', 'agency_name_secondary'
    ]
    
    hqta2 = hqta.dissolve(by=hqta_cols).reset_index()
    
    return hqta2


def spatial_join_facilities_hqta():
    facilities = catalog.tier1_facilities_processed.read()
    hqta = subset_hqta()    
    
    # spatial join
    gdf = gpd.sjoin(
        facilities.to_crs(geography_utils.WGS84),
        hqta.to_crs(geography_utils.WGS84),
        how = "inner",
        predicate = "intersects",
    ).drop(columns = "index_right").drop_duplicates()
    
    return gdf


def layers_to_plot():
    gdf = spatial_join_facilities_hqta()

    # Which locations
    facility_cols = ["sqft", "category", 
                     "facility_name", "facility_type",
                     "address_arcgis_clean", 
                     "county_name", "district",
                    ]
    facilities = gdf[["sheet_uuid", "geometry"] + facility_cols].drop_duplicates()
    
    
    # Which HQTA corridors (polygon geom)
    hqta_cols = [
        'calitp_itp_id_primary',
        'agency_name_primary', 
        'hqta_type', 
        'calitp_itp_id_secondary',
        'agency_name_secondary'
    ]

    hqta = subset_hqta()
    hqta_corr = (gdf[["sheet_uuid"] + facility_cols + hqta_cols].drop_duplicates()
                 .merge(hqta,
                        on = hqta_cols,
                        how = "inner"
                       )
                )

    hqta_corr = gpd.GeoDataFrame(hqta_corr).to_crs(geography_utils.WGS84)
    
    #facilities.to_parquet("./data/plot_facilities.parquet")
    #hqta_corr.to_parquet("./data/plot_hqta.parquet")
    
    return facilities, hqta_corr