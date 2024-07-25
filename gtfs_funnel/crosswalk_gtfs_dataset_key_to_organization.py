"""
When we publish our downstream outputs, we use the more stable
org_source_record_id and shed our internal modeling keys.

Currently, we save an output with our keys, then an output
without our keys to easily go back to our workflow whenever we get
feedback we get from users, but this is very redundant.
"""
import datetime
import pandas as pd

from shared_utils import schedule_rt_utils
from segment_speed_utils import helpers
from update_vars import GTFS_DATA_DICT, SCHED_GCS

import os
from calitp_data_analysis.sql import query_sql
from calitp_data_analysis.tables import tbls
from siuba import *

def create_gtfs_dataset_key_to_organization_crosswalk(
    analysis_date: str
) -> pd.DataFrame:
    """
    For every operator that appears in schedule data, 
    create a crosswalk that links to organization_source_record_id.
    For all our downstream outputs, at various aggregations,
    we need to attach these over and over again.
    """
    df = helpers.import_scheduled_trips(
        analysis_date,
        columns = ["gtfs_dataset_key", "name"],
        get_pandas = True
    ).rename(columns = {"schedule_gtfs_dataset_key": "gtfs_dataset_key"})
    # rename columns because we must use simply gtfs_dataset_key in schedule_rt_utils function
    
    # Get base64_url, organization_source_record_id and organization_name
    crosswalk = schedule_rt_utils.sample_gtfs_dataset_key_to_organization_crosswalk(
        df,
        analysis_date,
        quartet_data = "schedule",
        dim_gtfs_dataset_cols = ["key", "source_record_id", "base64_url"],
        dim_organization_cols = ["source_record_id", "name", 
                                 "itp_id", "caltrans_district",
                                  "ntd_id_2022"]
    )

    df_with_org = pd.merge(
        df.rename(columns = {"gtfs_dataset_key": "schedule_gtfs_dataset_key"}),
        crosswalk,
        on = "schedule_gtfs_dataset_key",
        how = "inner"
    )
    
    return df_with_org

def load_ntd(year: int) -> pd.DataFrame:
    """
    Load NTD Data stored in our warehouse.
    Select certain columns.
    """
    df = (
        tbls.mart_ntd.dim_annual_ntd_agency_information()
        >> filter(_.year == year, _.state == "CA", _._is_current == True)
        >> select(
            _.number_of_state_counties,
            _.uza_name,
            _.density,
            _.number_of_counties_with_service,
            _.state_admin_funds_expended,
            _.service_area_sq_miles,
            _.population,
            _.service_area_pop,
            _.subrecipient_type,
            _.primary_uza,
            _.reporter_type,
            _.organization_type,
            _.agency_name,
            _.voms_pt,
            _.voms_do,
            _.ntd_id,
            _.year,
        )
        >> collect()
    )

    cols = list(df.columns)
    df2 = df.sort_values(by=cols, na_position="last")
    df3 = df2.groupby("agency_name").first().reset_index()

    return df3

def load_mobility()->pd.DataFrame:
    """
    Load mobility data in our warehouse.
    """
    df = (
    tbls.mart_transit_database.dim_mobility_mart_providers()
     >> select(
        _.agency_name,
        _.counties_served,
        _.hq_city,
        _.hq_county,
        _.is_public_entity,
        _.is_publicly_operating,
        _.funding_sources,
        _.on_demand_vehicles_at_max_service,
        _.vehicles_at_max_service
    )
    >> collect()
    )
    
    df2 = df.sort_values(by=["on_demand_vehicles_at_max_service","vehicles_at_max_service"], ascending = [False, False])
    df3 = df2.groupby('agency_name').first().reset_index()
    return df3

def merge_ntd_mobility(year:int)->pd.DataFrame:
    ntd = load_ntd(year)
    mobility = load_mobility()
    m1 = pd.merge(
    mobility,
    ntd,
    how="inner",
    on="agency_name")
    agency_dict = {
    "City of Fairfield, California": "City of Fairfield",
    "Livermore / Amador Valley Transit Authority": "Livermore-Amador Valley Transit Authority",
    "Nevada County Transit Services": "Nevada County",
    "Omnitrans": "OmniTrans"}
    
    m1.agency_name = m1.agency_name.replace(agency_dict)
    m1.agency_name = m1.agency_name.str.strip()
    m1 = m1.drop_duplicates(subset = ["agency_name"]).reset_index(drop = True)
    m1 = m1.drop(columns = ["agency_name"])
    return m1

if __name__ == "__main__":

    from update_vars import analysis_date_list, ntd_latest_year
    
    EXPORT = GTFS_DATA_DICT.schedule_tables.gtfs_key_crosswalk
    start = datetime.datetime.now()
    
    for analysis_date in analysis_date_list:
        t0 = datetime.datetime.now()
        df = create_gtfs_dataset_key_to_organization_crosswalk(
            analysis_date
        )
        
        # Add some NTD data: if I want to delete this, simply take out
        # ntd_df and the crosswalk_df merge.
        ntd_df = merge_ntd_mobility(ntd_latest_year)
        
        crosswalk_df = pd.merge(df,
        ntd_df,
        left_on = ["ntd_id_2022"],
        right_on = ["ntd_id"],
        how = "left")
        
        # Drop ntd_id from ntd_df to avoid confusion
        crosswalk_df = crosswalk_df.drop(columns = ["ntd_id"])
        
        crosswalk_df.to_parquet(
            f"{SCHED_GCS}{EXPORT}_{analysis_date}.parquet"
        )
        t1 = datetime.datetime.now()
        print(f"finished {analysis_date}: {t1-t0}")
    
    end = datetime.datetime.now()
    print(f"execution time: {end - start}")

 