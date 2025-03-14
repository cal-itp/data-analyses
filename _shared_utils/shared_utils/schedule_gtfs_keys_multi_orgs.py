"""
Create the GTFS Digest yaml that
sets the parameterization for the analysis site.
"""
import sys

import pandas as pd

# import yaml
from shared_utils import portfolio_utils
from update_vars import GTFS_DATA_DICT

sys.path.append("../../gtfs_digest/")


def count_orgs(df: pd.DataFrame) -> list:
    """
    Count the number of unique organization_names
    to schedule_gtfs_dataset_keys. Filter out any
    schedule_gtfs_dataset_keys with less than 2 unique
    organization_names. Return these schedule_gtfs_dataset_keys
    in a list.
    """
    agg1 = (
        df.groupby(["caltrans_district", "schedule_gtfs_dataset_key"])
        .agg({"organization_name": "nunique"})
        .reset_index()
    )

    # Filter out rows with more than 1 organization_name
    agg1 = agg1.loc[agg1.organization_name > 1].reset_index(drop=True)
    # Grab schedule_gtfs_datset_key into a list
    multi_org_list = list(agg1.schedule_gtfs_dataset_key.unique())
    return multi_org_list


def find_schd_keys_multi_ops() -> pd.DataFrame:
    """
    Return a dataframe with all the schedule_gtfs_dataset_keys
    that have more than one organization_name that corresponds to it.
    This way, we won't include duplicate organizations when publishing
    our GTFS products.
    """
    schd_vp_url = f"{GTFS_DATA_DICT.digest_tables.dir}{GTFS_DATA_DICT.digest_tables.route_schedule_vp}.parquet"

    subset = [
        "caltrans_district",
        "schedule_gtfs_dataset_key",
        "organization_name",
        "service_date",
    ]

    sort_cols = [
        "caltrans_district",
        "service_date",
        "schedule_gtfs_dataset_key",
    ]

    schd_vp_df = pd.read_parquet(
        schd_vp_url,
        filters=[[("sched_rt_category", "in", ["schedule_and_vp", "schedule_only"])]],
        columns=subset,
    )

    # Sort dataframe to keep the  row for district/gtfs_key for the most
    # current date
    schd_vp_df2 = schd_vp_df.dropna(subset="caltrans_district").sort_values(by=sort_cols, ascending=[True, False, True])
    schd_vp_df3 = schd_vp_df2.drop_duplicates(
        subset=[
            "organization_name",
            "schedule_gtfs_dataset_key",
            "caltrans_district",
        ]
    )

    # Aggregate the dataframe to find schedule_gtfs_dataset_keys
    # With multiple organization_names.
    multi_orgs_list = count_orgs(schd_vp_df3)

    # Filter out the dataframe to only include schedule_gtfs_keys with multiple orgs
    schd_vp_df4 = schd_vp_df3.loc[schd_vp_df3.schedule_gtfs_dataset_key.isin(multi_orgs_list)].reset_index(drop=True)

    # Drop duplicates for organization_name
    schd_vp_df5 = schd_vp_df4.drop_duplicates(subset=["caltrans_district", "organization_name"]).reset_index(drop=True)

    # Aggregate the dataframe to find schedule_gtfs_dataset_keys
    # with multiple organization_names once more.
    multi_orgs_list2 = count_orgs(schd_vp_df5)

    # Filter one last time to only include schedule_gtfs_keys with multiple orgs
    schd_vp_df6 = schd_vp_df5.loc[schd_vp_df5.schedule_gtfs_dataset_key.isin(multi_orgs_list2)].reset_index(drop=True)

    # Clean
    schd_vp_df6 = schd_vp_df6.drop(columns=["service_date"])
    schd_vp_df6["combo"] = schd_vp_df6.caltrans_district + " (" + schd_vp_df6.schedule_gtfs_dataset_key + ")"

    return schd_vp_df6


SITE_YML = "./schedule_gtfs_dataset_key_multi_operator.yml"

if __name__ == "__main__":
    df = find_schd_keys_multi_ops()

    portfolio_utils.create_portfolio_yaml_chapters_with_sections(
        SITE_YML,
        df,
        chapter_info={
            "column": "combo",
            "name": "District/Key",
            "caption_prefix": "",
            "caption_suffix": "",
        },
        section_info={
            "column": "organization_name",
            "name": "organization_name",
        },
    )
