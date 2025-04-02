"""
Create the GTFS Digest yaml that 
sets the parameterization for the analysis site.
"""
from shared_utils import catalog_utils, portfolio_utils,  publish_utils
import pandas as pd
import _portfolio_names_dict
GTFS_DATA_DICT = catalog_utils.get_catalog("gtfs_analytics_data")

SITE_YML = "../portfolio/sites/gtfs_digest.yml"

def generate_operator_grain_yaml()->pd.DataFrame:
    """
    Generate the yaml for our Operator grain portfolio.
    """
    schd_vp_url = f"{GTFS_DATA_DICT.digest_tables.dir}{GTFS_DATA_DICT.digest_tables.route_schedule_vp}.parquet"
    
    # Keep only organizations with RT and schedule OR only schedule.
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

    
    # Drop duplicates & drop any rows without CT district values
    schd_vp_df = (schd_vp_df
                  .drop_duplicates(subset=[
            "schedule_gtfs_dataset_key",
            "caltrans_district",
            "organization_name",
            "name",
            "sched_rt_category",
        ]
    ).dropna(subset="caltrans_district")
     .reset_index(drop = True)
                 )
    
    # Get the most recent date using publish_utils
    recent_date = publish_utils.filter_to_recent_date(schd_vp_df)
    
    # Merge to get the most recent row for each organization
    schd_vp_df.service_date = schd_vp_df.service_date.astype(str)
    m1 = pd.merge(schd_vp_df, recent_date)
    
    # Map certain organizations for the portfolio name 
    m1["portfolio_name"] = m1.organization_name.map(_portfolio_names_dict.combined_names_dict)
    
    # Fill NA in new column with organization_name 
    m1.portfolio_name = m1.portfolio_name.fillna(m1.organization_name)
    
    # Drop duplicates again & sort
    final_cols = ["caltrans_district",
        "portfolio_name",]
    
    m2 = m1.drop_duplicates(
    subset= final_cols
    ).sort_values(by = final_cols, ascending = [True, True])
    
    final = m2[final_cols]
    
    return final

if __name__ == "__main__":
    final = generate_operator_grain_yaml()

    portfolio_utils.create_portfolio_yaml_chapters_with_sections(
        SITE_YML,
        final,
        chapter_info = {
            "column": "caltrans_district",
            "name": "district",
            "caption_prefix": "District ",
            "caption_suffix": "",
        },
        section_info = {
            "column": "portfolio_name",
            "name": "portfolio_name",
        },
    )