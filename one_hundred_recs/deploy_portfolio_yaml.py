"""
Run this script as part of the Makefile (in data-analyses).
Programmatically add params to how deploying portfolio,
especially when there are too many parameters to list.
"""
import pandas as pd
import yaml

from calitp.tables import tbls
from pathlib import Path
from siuba import *

PORTFOLIO_SITE_YAML = Path("./portfolio/sites/test_one_hundred_recs.yml")
    
NOTEBOOKS_TO_PARAMETERIZE = [
    './bus_service_increase/transit-on-shn.ipynb',
    './bus_service_increase/transit-deserts-uncompetitive.ipynb',
]

def overwrite_yaml(portfolio_site_yaml: Path) -> list:
    """
    portfolio_site_yaml: str
                        relative path to where the yaml is for portfolio
                        '../portfolio/analyses.yml' or '../portfolio/sites/parallel_corridors.yml'
    SITE_NAME: str
                name given to this analysis 
                'parallel_corridors', 'rt', 'dla'
    """
    districts = (
        tbls.airtable.california_transit_organizations()
        >> select(_.caltrans_district)
        >> distinct()
        >> filter(_.caltrans_district != None)
        >> arrange(_.caltrans_district)
        >> collect()
    ).caltrans_district.tolist()


    with open(portfolio_site_yaml) as analyses:
        analyses_data = yaml.load(analyses, yaml.Loader)

    # Loop through each district, grab the valid itp_ids
    # populate each dict key (caption, params, sections) needed to go into analyses.yml
    chapters_list = []
    for district in districts:
        chapter_dict = {}
        chapter_dict['caption'] = f'District {district}'
        chapter_dict['params'] = {'district': district}
        chapter_dict['sections'] = [{'notebook': i} for i in 
                                    NOTEBOOKS_TO_PARAMETERIZE]
        chapters_list += [chapter_dict]

    # Make this into a list item
    parts_list = [{'chapters': chapters_list}]

    analyses_data['parts'] = parts_list
    
    output = yaml.dump(analyses_data)

    with open(portfolio_site_yaml, 'w') as analyses:
        analyses.write(output)
    
    print("YAML for site generated")


if __name__ == "__main__":
    overwrite_yaml(PORTFOLIO_SITE_YAML)