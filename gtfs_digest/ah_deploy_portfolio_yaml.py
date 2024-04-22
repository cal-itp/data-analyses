import pandas as pd
import yaml

from pathlib import Path
from typing import Union

from segment_speed_utils.project_vars import RT_SCHED_GCS
from shared_utils import catalog_utils
GTFS_DATA_DICT = catalog_utils.get_catalog("gtfs_analytics_data")
PORTFOLIO_SITE_YAML = Path("../portfolio/sites/test_gtfs_exploratory.yml")

def overwrite_yaml(portfolio_site_yaml: Path) -> list:
    """
    portfolio_site_yaml: str
                        relative path to where the yaml is for portfolio
                        '../portfolio/analyses.yml' or '../portfolio/sites/parallel_corridors.yml'
    SITE_NAME: str
                name given to this analysis 
                'parallel_corridors', 'rt', 'dla'
    """
    # Load schedule_and_vp
    schd_vp_url = f"{GTFS_DATA_DICT.digest_tables.dir}{GTFS_DATA_DICT.digest_tables.route_schedule_vp}.parquet"
    
    schd_vp_df = (pd.read_parquet(schd_vp_url, 
                   filters=[[("sched_rt_category", "==", "schedule_and_vp")]],
                   columns = ["organization_name", "caltrans_district",
                                  "name", "schedule_gtfs_dataset_key"]
    )
                 )
    
    # Drop duplicated rows by Caltrans district and organization name.
    schd_vp_df = (schd_vp_df
                  .dropna(subset="caltrans_district")
                  .sort_values(by=["caltrans_district", "organization_name"])
                  .drop_duplicates()
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
    m1 = pd.merge(
    op_profiles4,
    schd_vp_df,
    on=["name", "organization_name", "schedule_gtfs_dataset_key"],
    how="inner"
    )
    
    districts = sorted(list(m1.caltrans_district.unique()))

    operators = m1.organization_name.tolist()      
    
    # Eric's example
    # https://github.com/cal-itp/data-analyses/blob/main/rt_delay/04_generate_all.ipynb

    with open(portfolio_site_yaml) as analyses:
        analyses_data = yaml.load(analyses, yaml.Loader)

    # Loop through each district, grab the valid itp_ids
    # populate each dict key (caption, params, sections) needed to go into analyses.yml
    chapters_list = []
    for district in districts:
        
        chapter_dict = {}
        subset = m1[m1.caltrans_district == district]
        
        chapter_dict['caption'] = f'District {district}'
        chapter_dict['params'] = {'district': district}
        chapter_dict['sections'] = [{'organization_name': name} for name in 
                                    subset.organization_name.unique().tolist()]
        chapters_list += [chapter_dict]

    # Make this into a list item
    parts_list = [{'chapters': chapters_list}]

    analyses_data['parts'] = parts_list
    
    output = yaml.dump(analyses_data)

    with open(portfolio_site_yaml, 'w') as analyses:
        analyses.write(output)
    
    print("YAML for site generated")
    
    return chapters_list


if __name__ == "__main__":
    rt_name_dict = overwrite_yaml(PORTFOLIO_SITE_YAML)
