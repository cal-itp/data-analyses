from shared_utils import catalog_utils, potfolio_utils
import pandas as pd

GTFS_DATA_DICT = catalog_utils.get_catalog("gtfs_analytics_data")
    
def operators_schd_vp_rt()->pd.DataFrame:
    """
    Operators who have schedule only OR have 
    both schedule and realtime data.
    """
    schd_vp_url = f"{GTFS_DATA_DICT.digest_tables.dir}{GTFS_DATA_DICT.digest_tables.route_schedule_vp}.parquet"
    
    schd_vp_df = pd.read_parquet(
        schd_vp_url, 
        filters=[[("sched_rt_category", "in", ["schedule_and_vp", "schedule_only"])]],
        columns = ["schedule_gtfs_dataset_key",
                   "caltrans_district", 
                   "organization_name", 
                   "name", 
                   "sched_rt_category", 
                   "service_date"]
    )
    
    schd_vp_df = schd_vp_df.assign(
        caltrans_district = schd_vp_df.caltrans_district.map(portfolio_utils.CALTRANS_DISTRICT_DICT)
    )
    
    # TODO: Check what the next step is doing? -
    # The line before got rid of 07 - Los Angeles and now uses new name 
   
    schd_vp_df = schd_vp_df.loc[schd_vp_df.caltrans_district != '07 - Los Angeles']
    
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
    
    
    final = schd_vp_df2[["caltrans_district","organization_name"]]
    return final