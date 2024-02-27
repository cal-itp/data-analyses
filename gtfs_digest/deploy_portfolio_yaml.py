import pandas as pd
import yaml

from pathlib import Path
from typing import Union

from segment_speed_utils.project_vars import RT_SCHED_GCS

PORTFOLIO_SITE_YAML = Path("../portfolio/sites/route_speeds.yml")
    
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
    
    operators_df = pd.read_parquet(f"{RT_SCHED_GCS}{DATASET}",
        columns = ["name"]
    ).sort_values("name").drop_duplicates()

    operators = operators_df.name.tolist()      
    # Eric's example
    # https://github.com/cal-itp/data-analyses/blob/main/rt_delay/04_generate_all.ipynb

    with open(portfolio_site_yaml) as analyses:
        analyses_data = yaml.load(analyses, yaml.Loader)

    # Loop through each district, grab the valid itp_ids
    # populate each dict key (caption, params, sections) needed to go into analyses.yml
    chapters_list = []
    for transit_operator in operators:
        
        chapter_dict = {}

        chapter_dict['caption'] = f'{transit_operator}'
        chapter_dict['params'] = {'name': transit_operator}
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