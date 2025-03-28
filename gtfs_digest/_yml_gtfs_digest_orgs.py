import pandas as pd
import yaml
from shared_utils import catalog_utils, portfolio_utils
import deploy_portfolio_yaml

GTFS_DATA_DICT = catalog_utils.get_catalog("gtfs_analytics_data")

"""
Dictionary
"""
reason_for_exclusion = {
    "City of Alameda": "Prefer using San Francisco Bay Area Water Emergency Transportation Authority (WETA) when displaying in our portflio."
}

"""
General Functions
"""
def df_to_yaml(
    df: pd.DataFrame, nest1_column: str, nest2_column: str, SITE_YML: str, title: str
):
    """
    Dump Pandas Dataframe to a YAML.

    Parameters:
    df (pd.DataFrame): DataFrame with 'sched_rt_category' and 'organization_name' columns.
    title (str): Title to be added at the top of the YAML file.

    Returns:
    yaml_str (str): YAML string representation of the input DataFrame.
    """
    # Initialize an empty dictionary to store the result
    result = {}

    # Iterate over unique  values in nest1_column
    for category in df[nest1_column].unique():
        # Filter the DataFrame for the current category
        category_df = df[df[nest1_column] == category]

        # Create a list of unique values in nest2_column for the current category
        organization_names = category_df[nest2_column].tolist()

        # Add the category and organization names to the result dictionary
        result[category] = organization_names

    # Save to YML
    with open(SITE_YML, "w") as f:
        f.write(f"# {title}\n\n")
        output = yaml.dump(result, default_flow_style=False)
        f.write(output)
    print("Saved to yml")
    
def count_orgs(df: pd.DataFrame, groupby_col: str, nunique_col: str) -> list:
    """
    Count the number of unique values the nunique_col
    to the groupby_col. Filter out any
    groupby_col with less than 2 unique
    values in nunique_col. Return these groupby_col values
    in a list.
    """
    agg1 = df.groupby([groupby_col]).agg({nunique_col: "nunique"}).reset_index()

    # Filter out rows with more than 1 organization_name
    agg1 = agg1.loc[agg1[nunique_col] > 1].reset_index(drop=True)

    # Grab groupby_col into a list
    multi_org_list = list(agg1[groupby_col].unique())
    return multi_org_list

"""
Functions to create YMLs
"""
def load_schd_vp() -> pd.DataFrame:
    schd_vp_url = f"{GTFS_DATA_DICT.digest_tables.dir}{GTFS_DATA_DICT.digest_tables.route_schedule_vp}.parquet"

    # Drop any duplicates
    schd_vp_df = (
        pd.read_parquet(
            schd_vp_url,
            columns=[
                "schedule_gtfs_dataset_key",
                "organization_name",
                "name",
                "sched_rt_category",
                "service_date",
            ],
        )
    ).drop_duplicates()

    # Sort values so the most current row stays at the top
    schd_vp_df = schd_vp_df.sort_values(
        by=[
            "organization_name",
            "service_date",
        ],
        ascending=[True, False],
    )
    
    # Drop duplicates again
    schd_vp_df = schd_vp_df.drop_duplicates(subset=["organization_name", "name"])

    # Drop service_date
    schd_vp_df = schd_vp_df.drop(columns=["service_date"]).reset_index(drop=True)

    # Fillna rows with None.
    schd_vp_df = schd_vp_df.fillna("None")
    
    # Filter out organization_names that are None.
    schd_vp_df = schd_vp_df.loc[schd_vp_df.organization_name != "None"].reset_index(
    drop=True
)
    return schd_vp_df



def generate_key_org_ymls(df: pd.DataFrame):
    """
    Generate the ymls that display the relationship
    between schedule_gtfs_dataset_key to organization_name
    values.
    """
    # One `organization_name` to many `schedule_gtfs_dataset_key`
    one_org_m_keys_list = count_orgs(
        df, "organization_name", "schedule_gtfs_dataset_key"
    )
    # Filter
    one_org_m_keys_df = df.loc[df.organization_name.isin(one_org_m_keys_list)].drop(
        columns=["sched_rt_category"]
    )

    # One `schedule_gtfs_dataset_key` to many `organization_name`
    one_key_many_orgs_list = count_orgs(
        df,
        "schedule_gtfs_dataset_key",
        "organization_name",
    )

    # Filter
    one_key_many_orgs_df = df.loc[
        df.schedule_gtfs_dataset_key.isin(one_key_many_orgs_list)
    ]

    # Merge them back together. This way we can find the many schedule_gtfs_dataset_key
    # to many organization_name values.
    m1 = pd.merge(
        one_org_m_keys_df,
        one_key_many_orgs_df,
        on=["schedule_gtfs_dataset_key", "organization_name", "name"],
        how="outer",
        indicator=True,
    )

    indicator_values = {
        "left_only": "1 organization_name:m schedule_gtfs_dataset_key",
        "right_only": "1 schedule_gtfs_dataset_key: m organization_name",
        "both": "m organization_name: m schedule_gtfs_datset_key",
    }
    m1._merge = m1._merge.map(indicator_values)

    # Re filter and save out to YML for each combo
    # One `organization_name` to many `schedule_gtfs_dataset_key`
    one_key_many_orgs_df = m1.loc[
        m1._merge == "1 schedule_gtfs_dataset_key: m organization_name"
    ]

    # Save to yml
    df_to_yaml(
        df=one_org_m_keys_df,
        nest1_column="organization_name",
        nest2_column="name",
        SITE_YML= "../_shared_utils/shared_utils/gtfs_digest_one_org_many_keys.yml",
        title="1 organization_name: m schedule_gtfs_dataset-key, all values below are encompassed under one organization_name",
    )

    # One `organization_name` to many `schedule_gtfs_dataset_key`
    one_org_m_keys_df = m1.loc[
        m1._merge == "1 organization_name:m schedule_gtfs_dataset_key"
    ]
    # Save to yml
    df_to_yaml(
        df=one_key_many_orgs_df,
        nest1_column="name",
        nest2_column="organization_name",
        SITE_YML="../_shared_utils/shared_utils/gtfs_digest_one_key_many_orgs.yml",
        title="1 schedule_gtfs_dataset_key:m organization_name, only the 1st value is displayed in the portfolio",
    )
    # Many organization_name to many schedule_gtfs_datset_keys"
    m_org_m_keys_df = m1.loc[
        m1._merge == "m organization_name: m schedule_gtfs_datset_key"
    ]
    # Save to yml
    df_to_yaml(
        df=m_org_m_keys_df,
        nest1_column="organization_name",
        nest2_column="name",
        SITE_YML="../_shared_utils/shared_utils/gtfs_digest_many_keys_many_orgs.yml",
        title="m schedule_gtfs_dataset_key:m organization_name",
    )
    
def generate_excluded_orgs_yml(df: pd.DataFrame):
    """
    Generate YML for excluded operators and explain why
    they aren't included.
    """
    # Manual list of operators we exclude
    operators_to_exclude = ["City of Alameda"]
    
    # Load in dataframe of organization_name values we display in our GTFS
    # Digest operator grain portfolio 
    orgs_in_portfolio = deploy_portfolio_yaml.generate_operator_grain_yaml()
    
    # Merge
    m1 = pd.merge(df, orgs_in_portfolio, how="outer", indicator=True)
    
    # Any left only values are excluded organizations 
    excluded_orgs = m1.loc[m1._merge == "left_only"]
    
    # Map dictionary of excluded operators
    excluded_orgs["reason_for_exclusion"] = excluded_orgs["organization_name"].map(
        reason_for_exclusion
    )
    
    # Subset
    excluded_orgs = excluded_orgs[["organization_name", "reason_for_exclusion"]]
    
    # Any organization without a manual value in the reason_for_exclusion dictionary
    # is excluded because it has another organization_name values that came before it 
    excluded_orgs = excluded_orgs.fillna(
        "1 schedule_gtfs_dataset_key:m organization_name, only 1st organization_name by alphabetical order is displayed in the portfolio"
    )

    # Generate YML
    df_to_yaml(
        df=excluded_orgs,
        nest1_column="reason_for_exclusion",
        nest2_column="organization_name",
        SITE_YML="../_shared_utils/shared_utils/gtfs_digest_excluded_orgs.yml",
        title="organization_name values that are excluded from the GTFS Digest portfolio",
    )
    
def generate_org_gtfs_status_yml(df: pd.DataFrame):
    # Subset
    df2 = df[
        [
            "sched_rt_category",
            "organization_name",
        ]
    ]
    # Generate YML 
    df_to_yaml(
        df2,
        "sched_rt_category",
        "organization_name",
        "../_shared_utils/shared_utils/gtfs_digest_org_gtfs_status.yml",
        "Operators who have RT (vp_only), Schedule, or Both (schedule_and_vp)",
    )
    
if __name__ == "__main__":
    df = load_schd_vp()
    generate_key_org_ymls(df)
    generate_excluded_orgs_yml(df)
    generate_org_gtfs_status_yml(df)