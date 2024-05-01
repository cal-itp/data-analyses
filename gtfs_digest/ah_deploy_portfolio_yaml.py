import pandas as pd
import yaml
import _operators_prep as op_prep
from pathlib import Path
from typing import Union

from segment_speed_utils.project_vars import RT_SCHED_GCS
from shared_utils import catalog_utils
GTFS_DATA_DICT = catalog_utils.get_catalog("gtfs_analytics_data")
PORTFOLIO_SITE_YAML = Path("../portfolio/sites/gtfs_digest.yml")

def overwrite_yaml(portfolio_site_yaml: Path) -> list:
    """
    portfolio_site_yaml: str
                        relative path to where the yaml is for portfolio
                        '../portfolio/analyses.yml' or '../portfolio/sites/parallel_corridors.yml'
    SITE_NAME: str
                name given to this analysis 
                'parallel_corridors', 'rt', 'dla'
    """
    m1 = op_prep.operators_with_rt()
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
