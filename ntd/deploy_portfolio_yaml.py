"""
Deploy portfolio yaml.

Since the names of RTPAs change so much depending on the crosswalk
we use, let's just generate the yaml.

Yaml structure is not nested by district, it is just all RTPAs
in the navigation panel.
"""
import pandas as pd
import yaml

from pathlib import Path
from update_vars import GCS_FILE_PATH

PORTFOLIO_SITE_YAML = Path("../portfolio/sites/ntd_monthly_ridership.yml")

def overwrite_yaml(portfolio_site_yaml: Path) -> list:
    """
    portfolio_site_yaml: str
                        relative path to where the yaml is for portfolio
                        '../portfolio/analyses.yml' or '../portfolio/sites/parallel_corridors.yml'
    SITE_NAME: str
                name given to this analysis 
                'parallel_corridors', 'rt', 'dla'
    """
    df = pd.read_parquet(
        f"{GCS_FILE_PATH}ca_monthly_ridership_2023_October.parquet",
        columns = ["RTPA"]
    ).drop_duplicates()

    with open(portfolio_site_yaml) as analyses:
        analyses_data = yaml.load(analyses, yaml.Loader)
    
    chapters_list = []
    
    for rtpa_name in sorted(df.RTPA.unique().tolist()):
        chapter_dict = {}
        chapter_dict["params"] = {"rtpa": rtpa_name}

        chapters_list += [chapter_dict]

    # Make this into a list item
    parts_list = [
        {"caption": "Introduction"},
        {"chapters": chapters_list}
    ]

    analyses_data['parts'] = parts_list
    
    output = yaml.dump(analyses_data)

    with open(portfolio_site_yaml, 'w') as analyses:
        analyses.write(output)
    
    print("YAML for site generated")
    
    return chapters_list

if __name__ == "__main__":
    overwrite_yaml(PORTFOLIO_SITE_YAML)