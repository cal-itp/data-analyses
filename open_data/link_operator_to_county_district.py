import geopandas as gpd
import intake
import pandas as pd

from calitp_data_analysis.sql import to_snakecase
from calitp_data_analysis import geography_utils
from update_vars import analysis_date, COMPILED_CACHED_VIEWS

catalog = intake.open_catalog("../_shared_utils/shared_utils/*.yml")
GCS_FILE_PATH = "gs://calitp-analytics-data/data-analyses/shared_data/"

def import_stops(analysis_date: str) -> gpd.GeoDataFrame:
    """
    Import stop-level data for a selected date.
    """
    stops = gpd.read_parquet(
        f"{COMPILED_CACHED_VIEWS}stops_{analysis_date}.parquet", 
        columns = ["feed_key", "stop_id", "geometry"]
    ).to_crs(geography_utils.WGS84)
    
    return stops


def import_ca_counties() -> gpd.GeoDataFrame:
    """
    Import CA counties from shared_utils/shared_data_catalog.yml.
    Just keep county_name for now.
    """
    ca_counties = (catalog.ca_counties.read()
                   .to_crs(geography_utils.WGS84)
                   .pipe(to_snakecase)
                   [["county_name", "geometry"]]
                  )
    
    dissolved = ca_counties.dissolve(
        by=["county_name"]).reset_index()
    
    return dissolved
    
    
def import_caltrans_districts() -> gpd.GeoDataFrame:
    """
    Import Caltrans districts from shared_utils/shared_data_catalog.yml.
    """
    caltrans_districts = (catalog.caltrans_districts.read()
                          .to_crs(geography_utils.WGS84)
                          .pipe(to_snakecase)
                          [["district", "geometry"]]
                         )
    
    return caltrans_districts


def sjoin_to_geography_aggregate_to_operator(
    stops: gpd.GeoDataFrame, 
    geography_polygon: gpd.GeoDataFrame, 
    geography_col: str
) -> gpd.GeoDataFrame:
    """
    Spatial join stops to see which ones fall within geography.
    Do a county by operator-geography (operator-county, operator-district).
    """
    stops_in_poly = gpd.sjoin(
        stops, 
        geography_polygon,
        how = "inner",
        predicate = "intersects"
    ).drop(columns = "index_right")
    
    count_stops_by_geog = geography_utils.aggregate_by_geography(
        stops_in_poly,
        group_cols = ["feed_key", geography_col],
        count_cols = ["stop_id"],
    ).sort_values(["feed_key", "stop_id"]).reset_index(drop=True)
    
    if geography_col == "district":
        geography_col_new = "district_str"
        count_stops_by_geog = count_stops_by_geog.assign(
            district_str = count_stops_by_geog[geography_col].astype(str)
        )
        
    else:
        geography_col_new = geography_col
    
    # Add a column that lists out the geographies, such as Los Angeles, Orange
    count_stops_by_geog = count_stops_by_geog.assign(
        geog_list = (count_stops_by_geog.sort_values(["feed_key", "stop_id"], 
                                                  ascending=[True, False])
                     .groupby("feed_key")
                     [geography_col_new]
                     .transform(lambda x: ', '.join(x))
                    )
    )
    
    return count_stops_by_geog


def get_predominant_geography(
    gdf: gpd.GeoDataFrame, 
    geography_col: str
) -> gpd.GeoDataFrame:
    """
    Associate the district or county for the operator by plurality.
    Also count the number of districts or counties that the operator 
    has stops in. 
    If the operator is predominantly in LA County, but also appears in Orange
    County, num_county will be 2.
    
    Attach name (from Airtable) too.
    """
    operator_predominant_geog = (
        gdf.sort_values(
        ["feed_key", "stop_id"], 
        ascending=[True, False])
        .drop_duplicates(subset="feed_key")
        [["feed_key", geography_col]]
        .reset_index(drop=True)
    )

    num_geog_units = (gdf.groupby(["feed_key", "geog_list"])
                      .agg({geography_col: "nunique"})
                      .reset_index()
                      .rename(columns = {
                          geography_col: f"num_{geography_col}", 
                          "geog_list": f"{geography_col}_list"
                      })
                    )
    
    operator_name = pd.read_parquet(
        f"{COMPILED_CACHED_VIEWS}trips_{analysis_date}.parquet",
        columns = ["feed_key", "name"]
    ).drop_duplicates()
    
    operator_by_geog = pd.merge(
        operator_predominant_geog,
        num_geog_units,
        on="feed_key",
        how = "inner",
        validate = "1:1"
    ).merge(
        operator_name,
        on = "feed_key",
        how = "inner"
    )
    
    return operator_by_geog


def get_operators_by_district(analysis_date: str):
    """
    High-level function for sjoin of stops to Caltrans districts, 
    and assigning which district is predominant for operator.
    """
    stops = import_stops(analysis_date)
    districts = import_caltrans_districts()
    
    stops_in_district = sjoin_to_geography_aggregate_to_operator(
        stops, 
        districts, 
        "district"
    )
    
    operator_by_district = get_predominant_geography(
        stops_in_district, "district")
    
    return operator_by_district
    
    
def get_operators_by_county(analysis_date: str):
    """
    High-level function for sjoin of stops to CA counties, 
    and assigning which county is predominant for operator.
    """
    stops = import_stops(analysis_date)
    counties = import_ca_counties()
    stops_in_counties= sjoin_to_geography_aggregate_to_operator(
        stops, 
        counties, 
        "county_name"
    )
    
    operator_by_county = get_predominant_geography(
        stops_in_counties, "county_name"
    ).rename(columns = {"num_county_name": "num_county"})
    
    return operator_by_county
    
    
if __name__ == "__main__":
    
    by_district = get_operators_by_district(analysis_date)
    by_county = get_operators_by_county(analysis_date)

    df = pd.merge(
        by_district,
        by_county,
        on = ["feed_key", "name"],
        how = "outer"
    ).drop(columns = "feed_key")
    
    col_order = list(df.columns)
    existing_col_order = [c for c in col_order if c != "name"]
    
    # where to export this to regularly and what file type?
    df.reindex(columns = ["name"] + existing_col_order).to_csv(
        f"{GCS_FILE_PATH}operator_by_district_county.csv", 
        index=False)

