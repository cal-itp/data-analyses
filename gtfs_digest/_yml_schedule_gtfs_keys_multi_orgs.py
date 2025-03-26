"""
Return a source of truth with organization names that correspond with one another.
There are instances in which  schedule_gtfs_dataset_keys
have more than one organization_name that corresponds to it.
We only want to publish a page for a schedule_gtfs_dataset_key once,
so we need a crosswalk to show something like City of Simi Valley is published
under City of Camarillo.
"""
import importlib
import os
import sys

import pandas as pd
import yaml

#sys.path.append(os.path.abspath("../../gtfs_digest/"))
#_operators_prep = importlib.import_module("_operators_prep")


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
        .agg({"repeated_organization_name": "nunique"})
        .reset_index()
    )

    # Filter out rows with more than 1 organization_name
    agg1 = agg1.loc[agg1.repeated_organization_name > 1].reset_index(drop=True)
    # Grab schedule_gtfs_datset_key into a list
    multi_org_list = list(agg1.schedule_gtfs_dataset_key.unique())
    return multi_org_list


def find_schd_keys_multi_ops() -> dict:
    # Load in the various dataframes that create the GTFS Digest portfolio site yaml
    all_categories, one_to_one_df, final = _operators_prep.operators_schd_vp_rt()

    # Subset and clean the dataframes
    subset_cols = ["schedule_gtfs_dataset_key", "caltrans_district", "organization_name"]

    # This dataframe displays the relationship of 1 schedule dataset key to many
    # organization names
    one_to_many_df = one_to_many_df[subset_cols]
    one_to_many_df = one_to_many_df.rename(columns={"organization_name": "repeated_organization_name"})

    # This dataframe displays the relationship of 1 schedule dataset key to 1
    # organization name
    one_to_one_df = one_to_one_df[subset_cols]
    one_to_one_df = one_to_one_df.rename(columns={"organization_name": "kept_organization_name"})
    # Merge the two dataframes
    m1 = pd.merge(
        one_to_one_df,
        one_to_many_df,
        on=["schedule_gtfs_dataset_key", "caltrans_district"],
    )

    # Find the schedule_dataset_keys with more than one organization_name
    # and filter out any rows that don't meet this criteria.
    multiple_organizations_list = count_orgs(m1)
    m2 = m1.loc[m1.schedule_gtfs_dataset_key.isin(multiple_organizations_list)]

    # Delete the rows that house the organization name we use for the portfolio
    m2["kept_name_bool"] = m2.kept_organization_name == m2.repeated_organization_name
    m3 = m2.loc[m2.kept_name_bool == False]

    # Clean and sort
    final_cols = ["kept_organization_name", "repeated_organization_name"]
    m3 = m3.sort_values(by=final_cols)[final_cols]

    # Turn it into a dictionary
    my_dict = m3.set_index("repeated_organization_name").T.to_dict("list")
    return my_dict


SITE_YML = "./_shared_utils/shared_utils/schedule_gtfs_dataset_key_multi_operator.yml"

if __name__ == "__main__":
    my_dict = find_schd_keys_multi_ops()

    with open(SITE_YML) as f:
        site_yaml_dict = yaml.load(f, yaml.Loader)

    output = yaml.dump(my_dict)

    with open(SITE_YML, "w") as f:
        f.write(output)
