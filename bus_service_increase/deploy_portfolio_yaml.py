"""
Run this script as part of the Makefile (in data-analyses).
Programmatically add params to how deploying portfolio,
especially when there are too many parameters to list.
"""
import geopandas as gpd
import intake
import pandas as pd
import yaml

from pathlib import Path
from typing import Union
from bus_service_utils import utils

ANALYSIS_DATE = "2022-05-04"
catalog = intake.open_catalog("./bus_service_increase/*.yml")

# these come from parallel_corridors_utils
# but, importing these throws error because of directories when the Makefile is run
PCT_COMPETITIVE_THRESHOLD = 0.5

PORTFOLIO_SITE_YAML = Path("./portfolio/sites/competitive_corridors.yml")

# Do a quick check and suppress operators that just show 1 route in each route_group
# From UI/UX perspective, it's confusing to readers because they think it's an error that
# more routes aren't showing up, rather than deliberately showing results that meet certain criteria
def valid_operators(df: Union[pd.DataFrame, gpd.GeoDataFrame]) -> list:
    t1 = df[df.pct_trips_competitive > PCT_COMPETITIVE_THRESHOLD]
    
    # Count unique routes that show up by operator-route_group
    t2 = (t1.groupby(["calitp_itp_id", "route_group"])
          .agg({"route_id":"nunique"})
          .reset_index()
         )
    
    # Valid if it's showing at least 2 routes in each group
    t2 = t2.assign(
        valid = t2.apply(lambda x: 1 if x.route_id > 1
                         else 0, axis=1)
    )
    
    # If all 3 groups are showing 1 route each, then that operator should be excluded from report
    t3 = t2.groupby("calitp_itp_id").agg({"valid": "sum"}).reset_index()

    t4 = t3[t3.valid >= 1]
    
    print(f"# operators included in analysis: {len(t3)}")
    print(f"# operators included in report: {len(t4)}")
   
    return list(t4.calitp_itp_id)
    
    
def overwrite_yaml(portfolio_site_yaml: Path) -> list:
    """
    portfolio_site_yaml: str
                        relative path to where the yaml is for portfolio
                        '../portfolio/analyses.yml' or '../portfolio/sites/parallel_corridors.yml'
    SITE_NAME: str
                name given to this analysis 
                'parallel_corridors', 'rt', 'dla'
    """
    # See example here how to inject and overwrite a paramter
    # https://github.com/intake/intake/pull/562
    df = catalog.competitive_route_variability(
        analysis_date = ANALYSIS_DATE
    ).read()  
    
    operators_to_include = valid_operators(df)
    
    df = df[df.calitp_itp_id.isin(operators_to_include)]
    
    districts = sorted(list(df[df.caltrans_district.notna()].caltrans_district.unique()))

    # Eric's example
    # https://github.com/cal-itp/data-analyses/blob/main/rt_delay/04_generate_all.ipynb

    with open(portfolio_site_yaml) as analyses:
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

    with open(portfolio_site_yaml, 'w') as analyses:
        analyses.write(output)
    
    print("YAML for site generated")
    
    return chapters_list


# Compare the ITP IDs for parallel corridors and RT
# If URL available for RT analysis, embed in parameterized notebook
def check_if_rt_data_available(portfolio_site_yaml: Path) -> dict:
    with open(portfolio_site_yaml) as analyses:
        analyses_data = yaml.load(analyses, yaml.Loader)
    
    rt_chapters = analyses_data['parts'][0]["chapters"]

    # Use a dict to capture what rank ITP ID is within that section
    # need to use it to construct URL
    rt_itp_ids_dict = {}
    
    for x, chapter in enumerate(rt_chapters):
        section_dict = chapter["sections"]
        for i, list_item in enumerate(section_dict):
            rt_itp_ids_dict[list_item["itp_id"]] = i

    return rt_itp_ids_dict



if __name__ == "__main__":
    itp_dict = overwrite_yaml(PORTFOLIO_SITE_YAML)