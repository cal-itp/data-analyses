import pandas as pd
import yaml

from pathlib import Path
from typing import Union

from segment_speed_utils.project_vars import analysis_date

PORTFOLIO_SITE_YAML = Path("../portfolio/sites/stop_segment_speeds.yml")
    
def overwrite_yaml(portfolio_site_yaml: Path) -> list:
    """
    portfolio_site_yaml: str
                        relative path to where the yaml is for portfolio
                        '../portfolio/analyses.yml' or '../portfolio/sites/parallel_corridors.yml'
    SITE_NAME: str
                name given to this analysis 
                'parallel_corridors', 'rt', 'dla'
    """
    operators_df = (pd.read_parquet(
            f"./scripts/data/stop_metrics_by_hour_{analysis_date}.parquet",
            columns = ["_gtfs_dataset_name"])
                 .sort_values("_gtfs_dataset_name")
                 .drop_duplicates()
                )
    operators_df = operators_df[~
                             operators_df._gtfs_dataset_name.str.contains(
                                 "LA Metro")]
    operators = operators_df._gtfs_dataset_name.tolist()      
    # Eric's example
    # https://github.com/cal-itp/data-analyses/blob/main/rt_delay/04_generate_all.ipynb

    with open(portfolio_site_yaml) as analyses:
        analyses_data = yaml.load(analyses, yaml.Loader)

    # Loop through each district, grab the valid itp_ids
    # populate each dict key (caption, params, sections) needed to go into analyses.yml
    chapters_list = []
    for transit_operator in operators:
        
        cleaned_name = (transit_operator.replace("VehiclePositions", "")
                        .replace("Vehicle Positions", "")
                       )
        
        chapter_dict = {}

        chapter_dict['caption'] = f'{cleaned_name}'
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