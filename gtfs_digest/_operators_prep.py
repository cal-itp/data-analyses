from shared_utils import catalog_utils, portfolio_utils
import pandas as pd

GTFS_DATA_DICT = catalog_utils.get_catalog("gtfs_analytics_data")
    
def operators_schd_vp_rt()->pd.DataFrame:
    """
    Generate the yaml for our portfolio.
    """
    schd_vp_url = f"{GTFS_DATA_DICT.digest_tables.dir}{GTFS_DATA_DICT.digest_tables.route_schedule_vp}.parquet"
    
    schd_vp_df = (pd.read_parquet(schd_vp_url,
                       columns = [ "schedule_gtfs_dataset_key",
                                    "caltrans_district",
                                    "organization_name",
                                    "name",
                                    "sched_rt_category",
                                    "service_date",]
                                     )
                     )

    schd_vp_df = schd_vp_df.assign(
        caltrans_district = schd_vp_df.caltrans_district.map(portfolio_utils.CALTRANS_DISTRICT_DICT)
    )
    
    # Sort/drop duplicates for only the most current row for each operator.
    agg1 = (
    schd_vp_df.dropna(subset="caltrans_district")
    .sort_values(
        by=[
            "caltrans_district",
            "organization_name",
            "service_date",
        ],
        ascending=[True, True, False],
    )
    .drop_duplicates(
        subset=[
            "organization_name",
            "caltrans_district",
        ]
    )
    .reset_index(drop=True)
    )
    # Manually filter out certain operators for schedule_gtfs_dataset_keys
    # that have multiple operators because keeping another value is preferable. 
    operators_to_exclude = ["City of Alameda"]
    agg1 = agg1.loc[~agg1.organization_name.isin(operators_to_exclude)].reset_index(drop = True)
    
    # Filter out any operators that are vp_only
    # This df retains multi orgs to one schedule gtfs dataset key
    one_to_many_df = (agg1
                     .loc[agg1.sched_rt_category
                     .isin(["schedule_and_vp","schedule_only"])]
                     .reset_index(drop = True)
                    )

    # Keep only one instance of a schedule_gtfs_dataset_key & subset
    final = (
    one_to_many_df.drop_duplicates(
        subset=[
            "schedule_gtfs_dataset_key",
        ]
    )
    .reset_index(drop=True)
    )[["caltrans_district","organization_name"]]
    
    return agg1, one_to_many_df, final