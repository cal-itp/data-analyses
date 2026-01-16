from functools import cache
from pathlib import Path

import pandas as pd
import google.auth

from calitp_data_analysis.gcs_pandas import GCSPandas
from calitp_data_analysis.sql import get_engine

from shared_utils import bq_utils

from update_vars import GTFS_DATA_DICT, file_name

# Initialize credentials and DB engine
credentials, project = google.auth.default()
db_engine = get_engine()


@cache
def gcs_pandas():
    return GCSPandas()
    
"""
Crosswalk
"""
def load_crosswalk() -> pd.DataFrame:
    df = bq_utils.download_table(
        project_name="cal-itp-data-infra",
        dataset_name="mart_transit_database",
        table_name="bridge_gtfs_analysis_name_x_ntd",
        date_col=None,
    )
    df2 = (
        df.dropna(subset=["ntd_id", "ntd_id_2022"])
        .drop_duplicates(
            subset=["analysis_name", "organization_name", "schedule_gtfs_dataset_name"]
        )
        .reset_index()
    )

    df2 = df2.rename(columns={"schedule_gtfs_dataset_name": "name"})

    df2["caltrans_district_int"] = df2.caltrans_district
    df2.caltrans_district = df2.caltrans_district.apply(lambda x: '{0:0>2}'.format(x)) 
    
    df2["caltrans_district"] = (
        df2.caltrans_district.astype(str) + "-" + df2.caltrans_district_name
    )

    df2 = df2[
        [
            "name",
            "analysis_name",
            "county_name",
            "caltrans_district",
            "caltrans_district_int",
            "ntd_id",
            "ntd_id_2022",
        ]
    ]

    gcs_pandas().data_frame_to_parquet(df2, f"{GTFS_DATA_DICT.gcs_paths.DIGEST_GCS}processed/{GTFS_DATA_DICT.gtfs_digest_rollup.crosswalk}_{file_name}.parquet")
    
    return df2


"""
NTD Data
"""
ntd_query_sql = f"""
        SELECT 
        number_of_state_counties,
        primary_uza_name,
        density,
        number_of_counties_with_service,
        state_admin_funds_expended,
        service_area_sq_miles,
        population,
        service_area_pop,
        subrecipient_type,
        primary_uza_code,
        reporter_type,
        organization_type,
        agency_name,
        voms_pt,
        voms_do,
        ntd_id,
        year
        FROM `cal-itp-data-infra-staging`.`mart_ntd`.`dim_annual_agency_information`
        WHERE state = 'CA' AND _is_current = TRUE
    """


mobility_query_sql = f"""
            SELECT
            agency_name,
            counties_served,
            hq_county,
            is_public_entity,
            is_publicly_operating,
            funding_sources,
            on_demand_vehicles_at_max_service,
            vehicles_at_max_service
            FROM
            cal-itp-data-infra.mart_transit_database.dim_mobility_mart_providers  
            """


def load_mobility(query: str) -> pd.DataFrame:
    with db_engine.connect() as connection:
        df = pd.read_sql(query, connection)
    df2 = df.sort_values(
        by=["on_demand_vehicles_at_max_service", "vehicles_at_max_service"],
        ascending=[False, False],
    )
    df3 = df2.groupby("agency_name").first().reset_index()
    return df3


def load_ntd(query: str) -> pd.DataFrame:
    with db_engine.connect() as connection:
        df = pd.read_sql(query, connection)
    df2 = df.sort_values(by=df.columns.tolist(), na_position="last")
    df3 = df2.groupby("agency_name").first().reset_index()
    return df3


def merge_ntd_mobility(ntd_query: str, mobility_query: str) -> pd.DataFrame:
    """
    Merge NTD (dim_annual_ntd_agency_information) with
    mobility providers (dim_mobility_mart_providers)
    and dedupe and keep 1 row per agency.
    """
    ntd = load_ntd(ntd_query)
    mobility = load_mobility(mobility_query)
    crosswalk = load_crosswalk()[["analysis_name", "ntd_id_2022"]]

    m1 = pd.merge(mobility, ntd, how="inner", on="agency_name")

    m1 = m1.drop_duplicates(subset="agency_name").reset_index(drop=True)

    # Wherever possible, allow nullable integers. These columns are integers, but can be
    # missing if we don't find corresponding NTD info
    integrify_cols = [
        "number_of_state_counties",
        "number_of_counties_with_service",
        "service_area_sq_miles",
        "service_area_pop",
        "on_demand_vehicles_at_max_service",
        "vehicles_at_max_service",
        "voms_pt",
        "voms_do",
        "year",
    ]
    m1[integrify_cols] = m1[integrify_cols].astype("Int64")

    # Merge with crosswalk to get analysis_name
    m1 = pd.merge(
        m1, crosswalk, left_on=["ntd_id"], right_on=["ntd_id_2022"], how="inner"
    )

    gcs_pandas().data_frame_to_parquet(m1,  f"{GTFS_DATA_DICT.gcs_paths.DIGEST_GCS}processed/{GTFS_DATA_DICT.gtfs_digest_rollup.ntd_profile}_{file_name}.parquet")

    return m1

if __name__ == "__main__":
    crosswalk_df = load_crosswalk()
    ntd_data_df = merge_ntd_mobility(
    ntd_query=ntd_query_sql, mobility_query=mobility_query_sql
)