from shared_utils import catalog_utils
import pandas as pd
import yaml

GTFS_DATA_DICT = catalog_utils.get_catalog("gtfs_analytics_data")
# Readable Dictionary
with open("readable.yml") as f:
    readable_dict = yaml.safe_load(f)

def operators_with_rt()->pd.DataFrame:
    schd_vp_url = f"{GTFS_DATA_DICT.digest_tables.dir}{GTFS_DATA_DICT.digest_tables.route_schedule_vp}.parquet"
    
    schd_vp_df = (pd.read_parquet(schd_vp_url, 
                       filters=[[("sched_rt_category", "==", "schedule_and_vp")]],
                       columns = [ "schedule_gtfs_dataset_key",
                                    "caltrans_district",
                                    "organization_name",
                                    "name",
                                    "sched_rt_category",
                                    "service_date",]
                                     )
                     )

    # Drop duplicated rows by Caltrans district and organization name.
    # Keep only the most recent row.
    schd_vp_df = (schd_vp_df
                      .dropna(subset="caltrans_district")
                      .sort_values(by=["caltrans_district", "organization_name", "service_date"],
            ascending=[False, False, False])
                      .drop(columns=["service_date"])
                      .drop_duplicates()
                      .reset_index(drop = True)
                     )

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
    # Merge 
    op_profiles_final = pd.merge(
        op_profiles4,
        schd_vp_df,
        on=["name", "organization_name", "schedule_gtfs_dataset_key"],
        how="inner"
        )
    return op_profiles_final