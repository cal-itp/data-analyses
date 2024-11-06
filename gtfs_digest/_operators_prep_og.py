from shared_utils import catalog_utils
import pandas as pd
import yaml

GTFS_DATA_DICT = catalog_utils.get_catalog("gtfs_analytics_data")
# Readable Dictionary
with open("readable.yml") as f:
    readable_dict = yaml.safe_load(f)
    
def operator_profiles()->pd.DataFrame:
    # Load operator profiles
    op_profiles_url = f"{GTFS_DATA_DICT.digest_tables.dir}{GTFS_DATA_DICT.digest_tables.operator_profiles}.parquet"
    op_cols = ["organization_name", "name", "service_date", "schedule_gtfs_dataset_key"]
    op_profiles_df = pd.read_parquet(op_profiles_url)[op_cols]

    # Keep the name with the most recent service date
    op_profiles2 = (op_profiles_df.sort_values(
        by=["name", "service_date"],
        ascending=[True, False])
                       )
    # Drop duplicated names
    op_profiles3 = op_profiles2.drop_duplicates(subset=["name"])

    # Drop duplicated organization names 
    op_profiles4  = (op_profiles3
                         .drop_duplicates(subset = ['organization_name'])
                         .reset_index(drop = True))
    return op_profiles4


def operators_schd_vp_rt()->pd.DataFrame:
    """
    Operators who have schedule only OR have 
    both schedule and realtime data.
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
    
    schd_vp_df3 = (
    schd_vp_df2.sort_values(
        by=["caltrans_district", "name", "service_date"], ascending=[True, False, False]
    )
    .drop_duplicates(subset=["caltrans_district", "name"])
    .reset_index(drop=True)
    )
    
    schd_vp_df3 = schd_vp_df3[["caltrans_district","organization_name"]]
    
    op_profile = operator_profiles()
    
    # Merge 
    final = pd.merge(
    schd_vp_df3, op_profile, on=["organization_name"],
        how="left")
    
    final = (final
             .sort_values(by = ["caltrans_district","organization_name"])
             .reset_index(drop = True)
            )
    return final
