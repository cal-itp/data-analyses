import intake
from calitp_data_analysis import geography_utils
from shared_utils import catalog_utils, rt_dates, rt_utils
import pandas as pd
import geopandas as gpd

catalog = intake.open_catalog("*.yml")

def read_census_tracts(
    analysis_date: str,
    crs: str = geography_utils.CA_NAD83Albers_m
) -> gpd.GeoDataFrame:
    census_tracts = (
        catalog.calenviroscreen_lehd_by_tract.read()
        .to_crs(crs)
        [["Tract", "pop_sq_mi", "Population",
          "geometry"]]
    ).rename(columns={'Tract':'tract', 'Population':'population'})
    return census_tracts