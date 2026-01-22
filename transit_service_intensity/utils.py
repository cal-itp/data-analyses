from functools import cache

import geopandas as gpd
import google.auth
import intake
from calitp_data_analysis import geography_utils
from calitp_data_analysis.gcs_geopandas import GCSGeoPandas
from update_vars import GCS_PATH, INPUT_GEOM_PATHS


@cache
def gcs_geopandas():
    return GCSGeoPandas()


catalog = intake.open_catalog("*.yml")
credentials, _ = google.auth.default()


def read_census_tracts(analysis_date: str, crs: str = geography_utils.CA_NAD83Albers_m) -> gpd.GeoDataFrame:
    census_tracts = (
        catalog.calenviroscreen_lehd_by_tract(geopandas_kwargs={"storage_options": {"token": credentials}})
        .read()
        .to_crs(crs)[["Tract", "pop_sq_mi", "Population", "geometry"]]
    ).rename(columns={"Tract": "tract", "Population": "population"})
    return census_tracts


def read_uzas(crs: str = geography_utils.CA_NAD83Albers_m):
    uza_cols = ["NAME", "UACE20", "geometry"]
    uza = gcs_geopandas().read_parquet(f'{GCS_PATH}{INPUT_GEOM_PATHS["urbanized_areas"]}')[uza_cols]
    uza.columns = uza.columns.str.lower()
    uza = uza.to_crs(crs)
    return uza
