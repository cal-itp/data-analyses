"""
When we publish our downstream outputs, we use the more stable
org_source_record_id and shed our internal modeling keys.

Currently, we save an output with our keys, then an output
without our keys to easily go back to our workflow whenever we get
feedback we get from users, but this is very redundant.
"""
import datetime
import pandas as pd

from shared_utils import schedule_rt_utils
from segment_speed_utils import helpers
from segment_speed_utils.project_vars import SCHED_GCS


def create_gtfs_dataset_key_to_organization_crosswalk(
    analysis_date: str
) -> pd.DataFrame:
    """
    For every operator that appears in schedule data, 
    create a crosswalk that links to organization_source_record_id.
    For all our downstream outputs, at various aggregations,
    we need to attach these over and over again.
    """
    df = helpers.import_scheduled_trips(
        analysis_date,
        columns = ["gtfs_dataset_key", "name"],
        get_pandas = True
    ).rename(columns = {"schedule_gtfs_dataset_key": "gtfs_dataset_key"})
    # rename columns because we must use simply gtfs_dataset_key in schedule_rt_utils function
    
    # Get base64_url, organization_source_record_id and organization_name
    crosswalk = schedule_rt_utils.sample_gtfs_dataset_key_to_organization_crosswalk(
        df,
        analysis_date,
        quartet_data = "schedule",
        dim_gtfs_dataset_cols = ["key", "base64_url"],
        dim_organization_cols = ["source_record_id", "name"]
    )

    df_with_org = pd.merge(
        df.rename(columns = {"gtfs_dataset_key": "schedule_gtfs_dataset_key"}),
        crosswalk,
        on = "schedule_gtfs_dataset_key",
        how = "inner"
    )
    
    return df_with_org


if __name__ == "__main__":

    from update_vars import analysis_date_list
    
    start = datetime.datetime.now()
    
    for analysis_date in analysis_date_list:
        t0 = datetime.datetime.now()
        df = create_gtfs_dataset_key_to_organization_crosswalk(
            analysis_date
        )
        
        df.to_parquet(
            f"{SCHED_GCS}crosswalk/"
            f"gtfs_key_organization_{analysis_date}.parquet"
        )
        t1 = datetime.datetime.now()
        print(f"finished {analysis_date}: {t1-t0}")
    
    end = datetime.datetime.now()
    print(f"execution time: {end - start}")

 