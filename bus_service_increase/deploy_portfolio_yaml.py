"""
Run this script as part of the Makefile (in data-analyses).
Programmatically add params to how deploying portfolio,
especially when there are too many parameters to list.
"""
import geopandas as gpd
import intake
import pandas as pd
import yaml

catalog = intake.open_catalog("./bus_service_increase/*.yml")

PORTFOLIO_SITE_YAML = "./portfolio/sites/parallel_corridors.yml"

def overwrite_yaml(PORTFOLIO_SITE_YAML):
    """
    PORTFOLIO_SITE_YAML: str
                        relative path to where the yaml is for portfolio
                        '../portfolio/analyses.yml' or '../portfolio/sites/parallel_corridors.yml'
    SITE_NAME: str
                name given to this analysis 
                'parallel_corridors', 'rt', 'dla'
    """
    df = catalog.competitive_route_variability.read()
    
    districts = sorted(list(df[df.caltrans_district.notna()].caltrans_district.unique()))[:1]

    # Eric's example
    # https://github.com/cal-itp/data-analyses/blob/main/rt_delay/04_generate_all.ipynb

    with open(PORTFOLIO_SITE_YAML) as analyses:
        analyses_data = yaml.load(analyses, yaml.Loader)
    
    # list any ITP IDs to be excluded, either because of invalid data or just too few results
    exclude_ids = [0]

    # Loop through each district, grab the valid itp_ids
    # populate each dict key (caption, params, sections) needed to go into analyses.yml
    chapters_list = []
    for district in districts:
        chapter_dict = {}
        subset = df[(df.caltrans_district == district) & 
                    (df.route_group.notna()) & 
                    (~df.calitp_itp_id.isin(exclude_ids))
                   ]
        chapter_dict['caption'] = f'District {district}'
        chapter_dict['params'] = {'district': district}
        chapter_dict['sections'] = [{'itp_id': itp_id} for itp_id in 
                                    subset.calitp_itp_id.unique().tolist()]
        chapters_list += [chapter_dict]

    # Make this into a list item
    parts_list = [{'chapters': chapters_list}]


    analyses_data['parts'] = parts_list
    
    output = yaml.dump(analyses_data)

    with open(PORTFOLIO_SITE_YAML, 'w') as analyses:
        analyses.write(output)
    
    print("YAML for site generated")
    
    return chapters_list

# Compare the ITP IDs for parallel corridors and RT
# If URL available for RT analysis, embed in parameterized notebook
def check_if_rt_data_available(PORTFOLIO_SITE_YAML):
    with open(PORTFOLIO_SITE_YAML) as analyses:
        analyses_data = yaml.load(analyses, yaml.Loader)
    
    rt_chapters = analyses_data['parts'][0]["chapters"]

    rt_itp_ids = []

    for x, chapter in enumerate(rt_chapters):
        section_dict = chapter["sections"]
        for i, list_item in enumerate(section_dict):
            rt_itp_ids.append(list_item['itp_id'])
            
    return rt_itp_ids


if __name__ == "__main__":
    itp_dict = overwrite_yaml(PORTFOLIO_SITE_YAML)