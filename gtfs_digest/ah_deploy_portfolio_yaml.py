import pandas as pd
import yaml

from pathlib import Path
from typing import Union

from segment_speed_utils.project_vars import RT_SCHED_GCS

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
    DATASET = f"digest/schedule_vp_metrics.parquet"
    
    df = pd.read_parquet(
        f"{RT_SCHED_GCS}{DATASET}",
        filters = [[("sched_rt_category", "==", "schedule_and_vp")]],
        columns = ["name", "caltrans_district"]
    ).dropna(
        subset="caltrans_district"
    ).sort_values(["caltrans_district", "name"])
    
    districts = sorted(list(df.caltrans_district.unique()))

    operators = df.name.tolist()      
    # Eric's example
    # https://github.com/cal-itp/data-analyses/blob/main/rt_delay/04_generate_all.ipynb

    with open(portfolio_site_yaml) as analyses:
        analyses_data = yaml.load(analyses, yaml.Loader)

    # Loop through each district, grab the valid itp_ids
    # populate each dict key (caption, params, sections) needed to go into analyses.yml
    chapters_list = []
    for district in districts:
        
        chapter_dict = {}
        subset = df[df.caltrans_district == district]
        
        chapter_dict['caption'] = f'District {district}'
        chapter_dict['params'] = {'district': district}
        chapter_dict['sections'] = [{'name': name} for name in 
                                    subset.name.unique().tolist()]
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
