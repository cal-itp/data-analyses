# !!DEPRECATED!! Switching to dim_organizations.rtpa_name

# Script to generate list of ntd_id crosswalked to Regional Transportation Planning Authority (RTPA) name
# joins map of CA census designated places (CDPs) to map of CA RTPAs
# ingest data from warehouse to get list of NTD agencies
# merge ntd agenices to list of CDPS/RTPAs by matching "City"

# decided to use `mart_ntd_funding_and_expenses.fct_service_data_and_operating_expenses_time_series_by_mode_upt` since it includeds more unique NTD ids, and is the same time frame for the annual ntd ridership report.

import geopandas as gpd
import pandas as pd
from calitp_data_analysis.tables import tbls
from calitp_data_analysis.sql import get_engine


GCS_FILE_PATH = "gs://calitp-analytics-data/data-analyses/ntd/"


# get data from warehouse
def get_ntd_agencies(min_year:str) -> pd.DataFrame:
    """
    reads in ntd data from warehouse, filters for CA agencies since 2018.
    groups data by agency and sum their UPT.
    """
    
    db_engine = get_engine()
    
    with db_engine.connect() as connection:
        query = """
            SELECT 
                `mart_ntd_funding_and_expenses.fct_service_data_and_operating_expenses_time_series_by_mode_upt_1`.`agency_name`, 
                `mart_ntd_funding_and_expenses.fct_service_data_and_operating_expenses_time_series_by_mode_upt_1`.`agency_status`, 
                `mart_ntd_funding_and_expenses.fct_service_data_and_operating_expenses_time_series_by_mode_upt_1`.`city`, 
                `mart_ntd_funding_and_expenses.fct_service_data_and_operating_expenses_time_series_by_mode_upt_1`.`legacy_ntd_id`, 
                `mart_ntd_funding_and_expenses.fct_service_data_and_operating_expenses_time_series_by_mode_upt_1`.`mode`, 
                `mart_ntd_funding_and_expenses.fct_service_data_and_operating_expenses_time_series_by_mode_upt_1`.`ntd_id`, 
                `mart_ntd_funding_and_expenses.fct_service_data_and_operating_expenses_time_series_by_mode_upt_1`.`reporter_type`, 
                `mart_ntd_funding_and_expenses.fct_service_data_and_operating_expenses_time_series_by_mode_upt_1`.`reporting_module`, 
                `mart_ntd_funding_and_expenses.fct_service_data_and_operating_expenses_time_series_by_mode_upt_1`.`service`, 
                `mart_ntd_funding_and_expenses.fct_service_data_and_operating_expenses_time_series_by_mode_upt_1`.`state`, 
                `mart_ntd_funding_and_expenses.fct_service_data_and_operating_expenses_time_series_by_mode_upt_1`.`uace_code`, 
                `mart_ntd_funding_and_expenses.fct_service_data_and_operating_expenses_time_series_by_mode_upt_1`.`primary_uza_name`, 
                `mart_ntd_funding_and_expenses.fct_service_data_and_operating_expenses_time_series_by_mode_upt_1`.`uza_population`, 
                `mart_ntd_funding_and_expenses.fct_service_data_and_operating_expenses_time_series_by_mode_upt_1`.`year`, 
                `mart_ntd_funding_and_expenses.fct_service_data_and_operating_expenses_time_series_by_mode_upt_1`.`upt` 
            FROM 
                `mart_ntd_funding_and_expenses.fct_service_data_and_operating_expenses_time_series_by_mode_upt` AS `mart_ntd_funding_and_expenses.fct_service_data_and_operating_expenses_time_series_by_mode_upt_1` 
            WHERE 
                (regexp_contains(`mart_ntd_funding_and_expenses.fct_service_data_and_operating_expenses_time_series_by_mode_upt_1`.`state`, 'CA') 
                OR regexp_contains(`mart_ntd_funding_and_expenses.fct_service_data_and_operating_expenses_time_series_by_mode_upt_1`.`state`, 'NV')) 
                AND `mart_ntd_funding_and_expenses.fct_service_data_and_operating_expenses_time_series_by_mode_upt_1`.`year` >= 2018 
                AND `mart_ntd_funding_and_expenses.fct_service_data_and_operating_expenses_time_series_by_mode_upt_1`.`city` IS NOT NULL 
                AND (regexp_contains(`mart_ntd_funding_and_expenses.fct_service_data_and_operating_expenses_time_series_by_mode_upt_1`.`primary_uza_name`, ', CA') 
                OR regexp_contains(`mart_ntd_funding_and_expenses.fct_service_data_and_operating_expenses_time_series_by_mode_upt_1`.`primary_uza_name`, 'CA-NV') 
                OR regexp_contains(`mart_ntd_funding_and_expenses.fct_service_data_and_operating_expenses_time_series_by_mode_upt_1`.`primary_uza_name`, 'California Non-UZA') 
                OR regexp_contains(`mart_ntd_funding_and_expenses.fct_service_data_and_operating_expenses_time_series_by_mode_upt_1`.`primary_uza_name`, 'El Paso, TX--NM'))            
                """
        get_ntd_time_series = pd.read_sql(query, connection)
    
    ntd_time_series = get_ntd_time_series.groupby(
        [
            "agency_name",
            'agency_status',
            "city",
            "state",
            "ntd_id",
            'primary_uza_name',
            "reporter_type",
            #"mode", # will need this for actual report, but will cause fan out. dont need that to create the crosswalk
            #"service", # will need this for actual report
        ]
    ).agg({
        "upt":"sum"
    }).sort_values(by="ntd_id").reset_index()
    
    return ntd_time_series


def get_cdp_to_rtpa_map(rtpa_url:str, cdp_url:str) -> pd.DataFrame:
    """
    reads in map of CA census designated places (CDPs)(polygon) and CA RTPA (polygon).
    Get centraiod of CDPS, then sjoin to RTPA map.
    Do some manual cleaning.
    """
    # RTPA map
    rtpa_path = rtpa_url
    rtpa_map = gpd.read_file(rtpa_path)[
        ["RTPA", "LABEL_RTPA", "geometry"]
    ]
    
    rtpa_map = rtpa_map.to_crs("ESRI:102600")  # for sjoin later

    # California Census Designated Places (2010), includes cities and CDPs
    cdp_path = cdp_url
    keep_cdp_col = ["FID", "NAME10", "NAMELSAD10", "geometry"]
    
    cdp_map = gpd.read_file(cdp_path)[keep_cdp_col].rename(
        columns={"NAME10": "cdp_name", "NAMELSAD10": "name_lsad"}
    ) 

    # get centroid of CPD geoms
    cdp_map["centroid"] = (
        cdp_map["geometry"].to_crs("ESRI:102600").centroid
    )
    
    cdp_points = cdp_map.set_geometry("centroid", drop=True)

    # sjoin
    city_to_rtpa = gpd.sjoin(
        cdp_points,  
        rtpa_map,
        how="left",
        predicate="intersects",
    )

    # Avalon fix
    city_to_rtpa.loc[city_to_rtpa["cdp_name"] == "Avalon", ("RTPA", "LABEL_RTPA")] = (
        "Southern California Association of Governments",
        "SCAG",
    )

    # San Francisco Fix
    city_to_rtpa.loc[
        city_to_rtpa["cdp_name"] == "San Francisco", ("RTPA", "LABEL_RTPA")
    ] = ("Metropolitan Transportation Commission", "MTC")
    
    return city_to_rtpa


def merge_agencies_to_rtpa_map(ntd_df:pd.DataFrame, city_rtpa_df:pd.DataFrame) -> pd.DataFrame:
    """
    merges the ntd data and rtpa data from `get_ntd_agencies` and `get_cdp_to_rtpa_map`.
    does some manual updating. 
    """
    # merge 
    alt_ntd_to_rtpa = ntd_df.merge(
        city_rtpa_df[["cdp_name", "RTPA"]],
        left_on=("city"),
        right_on=("cdp_name"),
        how="left",
        indicator=True,
    )

    # rows with NaN RTPAs
    alt_ntd_to_rtpa[alt_ntd_to_rtpa["RTPA"].isna()][
        ["agency_name", "city", "cdp_name", "RTPA"]
    ].drop_duplicates().sort_values(by="city") 


    #dictionary to update missing cdp and RTPA values from Cities

    update_dict={
        "Mcfarland": ("Mcfarland","Kern Council of Governments"),
        "Ventura":("Ventura","Southern California Association of Governments"),
        "Palos Verdes Peninsula":("Rolling Hills","Southern California Association of Governments"),# to match other entries for this agency
        "Havasu Lake":("Havasu Lake","Southern California Association of Governments"), # aka Lake Havasu. shares zip code with Needles. so update to SCAG
        "North Fork":("North Fork","Madera County Transportation Commission"), #in Madera County, update to
        "Montery Park":("Monterey Park","Southern California Association of Governments"),
        "Paso Robles":("Paso Robles","San Luis Obispo Council of Governments"),
        "Sherman Oaks":("Sherman Oaks","Southern California Association of Governments"),
        "Stateline":("Stateline", "Tahoe Regional Planning Agency"),
    }

    # loop to apply update_dict:
    for k,v in update_dict.items():
        alt_ntd_to_rtpa.loc[alt_ntd_to_rtpa["city"]==k,("cdp_name","RTPA")] = v
    
    # some dupe ntd need to be removed because their cities matched to duplicate cities (Burbank in NorCal and SoCal)
    remove_3 = (alt_ntd_to_rtpa["ntd_id"]=="90256") & (alt_ntd_to_rtpa["RTPA"]=="Metropolitan Transportation Commission")
    remove_4 = (alt_ntd_to_rtpa["ntd_id"]=="90287") & (alt_ntd_to_rtpa["RTPA"]=="Madera County Transportation Commission")

    alt_ntd_to_rtpa = alt_ntd_to_rtpa[~(remove_3 | remove_4)]
    
    return alt_ntd_to_rtpa


def make_export_clean_crosswalk(df:pd.DataFrame) -> pd.DataFrame:
    # final crosswalk
    ntd_data_to_rtpa_cleaned = alt_ntd_to_rtpa[
        ["ntd_id","agency_name","reporter_type","agency_status","city","state","RTPA"]
    ].drop_duplicates(subset=["ntd_id"]).reset_index(drop=True)


    ntd_data_to_rtpa_cleaned.to_parquet(f"{GCS_FILE_PATH}ntd_id_rtpa_crosswalk_all_reporter_types.parquet")
    ntd_data_to_rtpa_cleaned.to_csv(f"{GCS_FILE_PATH}ntd_id_rtpa_crosswalk_all_reporter_types.csv")

if __name__ == "__main__":
    print("get list of ntd agencies")
    ntd_time_series = get_ntd_agencies(min_year="2018")
    
    print("get list census designated places to rtpa map")
    city_to_rtpa = get_cdp_to_rtpa_map(
        rtpa_url="https://cecgis-caenergy.opendata.arcgis.com/api/download/v1/items/3a83743378be4e7f84c8230889c01dea/geojson?layers=0",
        cdp_url="https://services6.arcgis.com/YBp5dUuxCMd8W1EI/arcgis/rest/services/California_Census_Designated_Places_2010/FeatureServer/0/query?outFields=*&where=1%3D1&f=geojson"
    )
    
    print("merge ntd agencies to cdp/rtpa map")
    alt_ntd_to_rtpa = merge_agencies_to_rtpa_map(
        ntd_df=ntd_time_series, 
        city_rtpa_df=city_to_rtpa
    )
    
    print("make clean crosswalk, export to GCS")
    make_export_clean_crosswalk(
        df=alt_ntd_to_rtpa
    )
    
    print("end script")