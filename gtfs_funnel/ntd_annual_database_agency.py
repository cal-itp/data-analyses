import pandas as pd
import os
from calitp_data_analysis.sql import query_sql
from calitp_data_analysis.tables import tbls
from siuba import *

def load_ntd(year: int) -> pd.DataFrame:
    """
    Load NTD Data stored in our warehouse.
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
    
    # SHOULD THE RESULTS BE SAVED OUT TO GCS?
    return m1
