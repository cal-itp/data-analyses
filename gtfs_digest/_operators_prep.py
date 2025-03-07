
from shared_utils import catalog_utils, portfolio_utils
import pandas as pd

GTFS_DATA_DICT = catalog_utils.get_catalog("gtfs_analytics_data")
    
def operators_schd_vp_rt()->pd.DataFrame:
    """
    Generate the yaml for our portfolio.
    """
    schd_vp_url = f"{GTFS_DATA_DICT.digest_tables.dir}{GTFS_DATA_DICT.digest_tables.route_schedule_vp}.parquet"
    

    schd_vp_df = (pd.read_parquet(schd_vp_url, 
                       filters=[[("sched_rt_category", "in", ["schedule_and_vp", "schedule_only"])]],
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

    schd_vp_df2 = (
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
    schd_vp_df2 = schd_vp_df2.loc[~schd_vp_df2.organization_name.isin(operators_to_exclude)]
    
    # Keep only one instance of a schedule_gtfs_dataset_key
    schd_vp_df3 = (
    schd_vp_df2.drop_duplicates(
        subset=[
            "schedule_gtfs_dataset_key",
        ]
    )
    .reset_index(drop=True)
    )
    
    final = schd_vp_df3[["caltrans_district","organization_name"]]
    return final