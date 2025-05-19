import pandas as pd
import merge_data
import numpy as np
import geopandas as gpd
import yaml
import deploy_portfolio_yaml
from shared_utils import catalog_utils, portfolio_utils, publish_utils
from update_vars import GTFS_DATA_DICT, RT_SCHED_GCS, SCHED_GCS, SEGMENT_GCS
import _portfolio_names_dict

GTFS_DATA_DICT = catalog_utils.get_catalog("gtfs_analytics_data")

"""
Columns & URLs for operator grain digest
"""
operator_digest_cols = [
    "schedule_gtfs_dataset_key",
    "caltrans_district",
    "organization_name",
    "name",
    "sched_rt_category",
    "service_date",
]

schd_vp_url = f"{GTFS_DATA_DICT.digest_tables.dir}{GTFS_DATA_DICT.digest_tables.route_schedule_vp}.parquet"

"""
General Functions
"""
def df_to_yaml(
    df: pd.DataFrame, 
    nest1_column: str, 
    nest2_column: str, 
    SITE_YML: str, 
    title: str
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
def load_df_for_yml(url: str, columns_to_keep=list) -> pd.DataFrame:
    # Keep only organizations with RT and schedule OR only schedule.
    df = pd.read_parquet(
        url,
        columns=columns_to_keep,
    )

    df = (
        df.drop_duplicates(subset=columns_to_keep)
        .reset_index(drop=True)
        .dropna(subset=["caltrans_district"])
    )

    # Get the most recent date using publish_utils
    recent_date = publish_utils.filter_to_recent_date(df)

    # Merge to get the most recent row for each organization
    df.service_date = df.service_date.astype(str)
    m1 = pd.merge(df, recent_date)

    # Map portfolio names
    m1["portfolio_name"] = m1.organization_name.map(
        _portfolio_names_dict.combined_names_dict
    )

    m1.portfolio_name = m1.portfolio_name.fillna(m1.organization_name)

    m1 = (
        m1.sort_values(
            by=[
                "service_date",
                "caltrans_district",
                "organization_name",
                "portfolio_name",
            ],
            ascending=[False, True, True, True],
        )
        .drop_duplicates(
            subset=["caltrans_district", "organization_name", "name", "portfolio_name"]
        )
        .reset_index(drop=True)
    )
    return m1

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
    one_org_m_keys_df = df.loc[df.organization_name.isin(one_org_m_keys_list)]

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
        on=["schedule_gtfs_dataset_key", "organization_name", "portfolio_name","name"],
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
        nest1_column="portfolio_name",
        nest2_column="name",
        SITE_YML= "../_shared_utils/shared_utils/gtfs_digest_one_org_many_keys.yml",
        title="1 organization_name: m schedule_gtfs_dataset-key, all values below are encompassed under one portfolio_name",
    )

    # One `schedule_gtfs_dataset_key` to many `organization_name`
    one_org_m_keys_df = m1.loc[
        m1._merge == "1 organization_name:m schedule_gtfs_dataset_key"
    ]
    # Save to yml
    df_to_yaml(
        df=one_key_many_orgs_df,
        nest1_column="portfolio_name",
        nest2_column="organization_name",
        SITE_YML="../_shared_utils/shared_utils/gtfs_digest_one_key_many_orgs.yml",
        title="1 schedule_gtfs_dataset_key:m organization_name: m organization_names are captured under portfolio_name",
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

    
def generate_org_gtfs_status_yml(df: pd.DataFrame):
    """
    Generate which operators have realtime data,
    schedule and realtime data, or realtime only.
    """
    # Subset
    df2 = df[
        [
            "sched_rt_category",
            "portfolio_name",
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
    
    return df2

def red_flags(
    df: pd.DataFrame,
    operators_to_keep: pd.DataFrame,
    group_cols: list,
    agg_col: str,
    agg_type: str,
    dataset_name: str,
) -> pd.DataFrame:
    """
    Aggregate data for YAML file.

    Parameters:
    df (pd.DataFrame): Input dataframe.
    operators_to_keep (pd.DataFrame): Operators to keep.
    group_cols (list): Group columns.
    agg_col (str): Aggregate column.
    agg_type (str): Aggregate type.
    dataset_name (str): Dataset name.

    Returns:
    pd.DataFrame: Aggregated dataframe.
    """
    # Merge dataframes
    merged_df = pd.merge(df, operators_to_keep, on=["organization_name"], how="left")

    # Aggregate data
    agg_df = merged_df.groupby(group_cols).agg({agg_col: agg_type}).reset_index()

    # Sort values
    agg_df = agg_df.sort_values(by=["portfolio_name", "service_date"])

    # Pivot data
    pivot_df = agg_df.pivot(
        index=["portfolio_name", "organization_name"],
        columns="service_date",
        values=agg_col,
    ).reset_index()

    try:
        # Calculate columns
        two_month_col = pivot_df.columns[-3]
        last_month_col = pivot_df.columns[-2]
        current_month_col = pivot_df.columns[-1]

        pivot_df["current_last_month_pct"] = (
            (pivot_df[current_month_col] - pivot_df[last_month_col])
            / (pivot_df[current_month_col])
            * 100
        )

        pivot_df["current_two_month_pct"] = (
            (pivot_df[current_month_col] - pivot_df[two_month_col])
            / (pivot_df[current_month_col])
            * 100
        )

        # Create flag column
        pivot_df["flag"] = np.where(
            (pivot_df["current_last_month_pct"] >= 20)
            | (pivot_df["current_two_month_pct"] >= 20)
            | (pivot_df["current_last_month_pct"] <= -20)
            | (pivot_df["current_two_month_pct"] <= -20),
            "check",
            "ok",
        )

        # Filter out rows
        filtered_df = pivot_df.loc[pivot_df["flag"] == "check"]

        # Create new columns
        filtered_df["dataset"] = dataset_name

        filtered_df["trend"] = (
            f"Unique rows for {current_month_col} based on {agg_col}: "
            + filtered_df[current_month_col].astype(str)
            + ", Last Month: "
            + filtered_df[last_month_col].astype(str)
            + ",  2 Months Ago: "
            + filtered_df[two_month_col].astype(str)
        )

        filtered_df = filtered_df[["portfolio_name", "dataset", "trend"]]
    except:
        filtered_df = pd.DataFrame(columns=["portfolio_name","dataset", "trend"])
        filtered_df.trend = f"All organizations are missing data for {dataset_name} for the most current month."
    return filtered_df

def prep_merge_data_script(df: pd.DataFrame, analysis_date_list: list) -> pd.DataFrame:
    """
    CLean up the datasets that go into creating
    gtfs_digest/merge_data.py
    """
    df_crosswalk = merge_data.concatenate_crosswalk_organization(analysis_date_list)[
        ["schedule_gtfs_dataset_key", "service_date", "organization_name"]
    ]

    m1 = pd.merge(df, df_crosswalk)

    m1 = m1.sort_values(by=["service_date", "organization_name"], ascending=False)
    return m1

def load_datasets(date_subset: list) -> pd.DataFrame:
    # Load DataFrames
    op_routes_url = f"{GTFS_DATA_DICT.digest_tables.dir}{GTFS_DATA_DICT.digest_tables.operator_routes_map}.parquet"
    op_routes_gdf = gpd.read_parquet(op_routes_url)
    op_routes_gdf = op_routes_gdf.loc[op_routes_gdf.service_date.isin(date_subset)]

    op_profiles_url = f"{GTFS_DATA_DICT.digest_tables.dir}{GTFS_DATA_DICT.digest_tables.operator_profiles}.parquet"
    op_profiles_df = pd.read_parquet(op_profiles_url)
    op_profiles_df.organization_name = op_profiles_df.organization_name.fillna("None")
    op_profiles_df = op_profiles_df.loc[op_profiles_df.service_date.isin(date_subset)]

    # Create some crosswalks bc certain datasets don't have columns
    name_org_name_crosswalk = op_routes_gdf[
        ["name", "organization_name"]
    ].drop_duplicates()

    scheduled_service_hours_url = f"{GTFS_DATA_DICT.digest_tables.dir}{GTFS_DATA_DICT.digest_tables.scheduled_service_hours}.parquet"
    scheduled_service_hours_df = pd.read_parquet(scheduled_service_hours_url)
    scheduled_service_hours_df = pd.merge(
        scheduled_service_hours_df, name_org_name_crosswalk, how="left"
    ).rename(columns={"month_year": "service_date"})

    df_sched = merge_data.concatenate_schedule_by_route_direction(date_subset)[
        ["service_date", "schedule_gtfs_dataset_key", "route_id"]
    ]
    df_sched = prep_merge_data_script(df_sched, date_subset)

    df_avg_speeds = merge_data.concatenate_speeds_by_route_direction(date_subset)[
        ["service_date", "schedule_gtfs_dataset_key", "route_id"]
    ]
    df_avg_speeds = prep_merge_data_script(df_avg_speeds, date_subset)

    df_rt_sched = merge_data.concatenate_rt_vs_schedule_by_route_direction(date_subset)[
        ["service_date", "schedule_gtfs_dataset_key", "route_id"]
    ]
    df_rt_sched = prep_merge_data_script(df_rt_sched, date_subset)
    return (
        op_routes_gdf,
        op_profiles_df,
        scheduled_service_hours_df,
        df_sched,
        df_avg_speeds,
        df_rt_sched,
    )

def generate_all_red_flags(dates_subset:list)->pd.DataFrame:
    
    # Load DataFrame
    (
        op_routes_gdf,
        op_profiles_df,
        scheduled_service_hours_df,
        df_sched,
        df_avg_speeds,
        df_rt_sched,
    ) = load_datasets(dates_subset)
    
    # Load in operators to keep
    ops_kept = deploy_portfolio_yaml.generate_operator_grain_yaml() 
    
    # Filter for only operators that we display in our portfolio
    # Aggregate to see if the rows for the most current month is 20% more or less
    # than the past month and two months ago. 
    op_routes_agg = red_flags(
    df=op_routes_gdf,
    operators_to_keep=ops_kept,
    group_cols=["service_date", "portfolio_name", "organization_name"],
    agg_col="route_id",
    agg_type="nunique",
    dataset_name="GTFS_DATA_DICT.digest_tables.operator_routes_map",
)
    op_profiles_agg = red_flags(
    df=op_profiles_df,
    operators_to_keep=ops_kept,
    group_cols=["service_date", "organization_name", "portfolio_name"],
    agg_col="operator_n_routes",
    agg_type="max",
    dataset_name="GTFS_DATA_DICT.digest_tables.operator_profiles",
)
    scheduled_service_hours_agg = red_flags(
    df=scheduled_service_hours_df,
    operators_to_keep=ops_kept,
    group_cols=["service_date", "organization_name", "portfolio_name"],
    agg_col="departure_hour",
    agg_type="nunique",
    dataset_name="GTFS_DATA_DICT.digest_tables.scheduled_service_hours",
)
    df_sched_agg = red_flags(
    df=df_sched,
    operators_to_keep=ops_kept,
    group_cols=["service_date", "organization_name", "portfolio_name"],
    agg_col="route_id",
    agg_type="nunique",
    dataset_name="merge_data.py/concatenate_schedule_by_route_direction",
)
    
    df_avg_speeds_agg = red_flags(
    df=df_avg_speeds,
    operators_to_keep=ops_kept,
    group_cols=["service_date", "organization_name", "portfolio_name"],
    agg_col="route_id",
    agg_type="nunique",
    dataset_name="merge_data.py/concatenate_speeds_by_route_direction",
)
    
    df_rt_sched_agg = red_flags(
    df=df_rt_sched,
    operators_to_keep=ops_kept,
    group_cols=["service_date", "organization_name", "portfolio_name"],
    agg_col="route_id",
    agg_type="nunique",
    dataset_name="merge_data.concatenate_rt_vs_schedule_by_route_direction",
)
    
    # Concat 
    final = pd.concat(
    [
        op_routes_agg,
        df_sched_agg,
        df_avg_speeds_agg,
        df_rt_sched_agg,
        op_profiles_agg,
        scheduled_service_hours_agg,
    ],
    ignore_index=True,
)
    
    # Clean
    final = final.sort_values(by=["portfolio_name", "dataset"]).reset_index()
    
    return final

def case1_vp_only(
    schedule_file: str,
    speeds_file: str
) -> list:
    """
    Find all operators that appear in avg_speeds that never appears in scheduled...
    then show the dates that are present for vp_only
    """
    '''
    scheduled_operators = pd.read_parquet(
        (most_recent)_schedule_file, 
    ).schedule_gtfs_dataset_key.unique().tolist()
    
    vp_operators pd.read_parquet(
        speeds_file, 
        columns = ["schedule_gtfs_dataset_key"],
        filters = [[("schedule_gtfs_dataset_key", "notin", scheduled_opeartors)]]
    ).drop_duplicates().pipe(publish_utils.filter_to_recent_date, ["schedule_gtfs_dataset_key"])
        
    '''
    return 
 
def case2_lost_rt(filepath):
    """
    schedule_and_rt_df, read in operator, operator's sched_rt_category based on mode(sched_rt_category) per date, 

     subset for our 2 types
     for sched_rt_category, group by operator, get max(date)
     for sched_category, group by operator, get max(date)
     merge these and filter if sched_category.date > sched_rt_category.date
     
    df should have name, sched_category's max date, sched_rt_category max(date)
    """
     return df

    
def case3_stale_data(filepath):
    """
    Find where we haven't found operators for at least 3 months
    """
    '''
    df = pd.read_parquet(
        most_recent_operator_data?,
        columns = ["name", "service_date"]
    )
    filter if it is at least 3 months old

    return df
    '''
if __name__ == "__main__":
    df = load_df_for_yml(schd_vp_url, operator_digest_cols)
    generate_key_org_ymls(df)
    generate_org_gtfs_status_yml(df)