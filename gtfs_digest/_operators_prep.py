from shared_utils import catalog_utils, portfolio_utils
import pandas as pd

GTFS_DATA_DICT = catalog_utils.get_catalog("gtfs_analytics_data")
    
def load_schd_vp_df(filter_schd_both:bool=True)->pd.DataFrame:
    """
    Load and sort route_schedule_vp
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
    
    # Manually filter out certain operators for schedule_gtfs_dataset_keys
    # that have multiple operators because keeping another value is preferable. 
    operators_to_exclude = ["City of Alameda"]
    schd_vp_df =schd_vp_df.loc[~schd_vp_df
                               .organization_name.isin
                               (operators_to_exclude)].reset_index(drop = True)
    
    # Sort 
    schd_vp_df = (schd_vp_df.dropna(subset="caltrans_district")
    .sort_values(
        by=[
            "caltrans_district",
            "organization_name",
            "service_date",
        ],
        ascending=[True, True, False],
    )
                 )
        
    if filter_schd_both == True:
        schd_vp_df = (schd_vp_df
                     .loc[schd_vp_df.sched_rt_category
                     .isin(["schedule_and_vp","schedule_only"])]
                     .reset_index(drop = True)
                    )

    return schd_vp_df