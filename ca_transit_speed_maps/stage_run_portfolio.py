import sys
# https://docs.python.org/3/library/warnings.html
if not sys.warnoptions:
    import warnings
    warnings.simplefilter("ignore")

from tqdm import tqdm
import time

import pandas as pd
from siuba import *

import datetime as dt
from shared_utils import rt_utils

import os

import pyaml
import yaml
from update_vars_index import PROGRESS_PATH

def make_rt_site_yml(speedmaps_index_joined,
                       rt_site_path = '../portfolio/sites/rt.yml'):
        
    # make sure index is generated
    assert speedmaps_index_joined.status.isin(['speedmap_segs_available']).all(), 'must run prior scripts first, see Makefile'
    
    with open(rt_site_path) as rt_site:
        rt_site_data = yaml.load(rt_site, yaml.Loader)
    
    chapters_list = []
    speedmaps_index_joined = speedmaps_index_joined >> arrange(_.caltrans_district)
    speedmaps_index_joined = speedmaps_index_joined >> distinct(_.caltrans_district, _.organization_name, _.organization_source_record_id)
    for district in speedmaps_index_joined.caltrans_district.unique():
        if type(district) == type(None):
            continue
        chapter_dict = {}
        filtered = (speedmaps_index_joined
                    >> filter(_.caltrans_district == district)
                    >> arrange(_.organization_name)
                   )
        chapter_dict['caption'] = f'District {district}'
        chapter_dict['params'] = {'district': district}
        chapter_dict['sections'] = \
            [{'organization_name': organization_name} for organization_name in filtered.organization_name.to_list()]
        chapters_list += [chapter_dict]   
        
    parts_list = [{'chapters': chapters_list}]
    rt_site_data['parts'] = parts_list
    
    output = pyaml.dump(rt_site_data)
    with open(rt_site_path, 'w') as rt_site:
        rt_site.write(output)
    
    print(f'portfolio yml staged to {rt_site_path}')
    return

def stage_portfolio():
    
    os.chdir('/home/jovyan/data-analyses')
    os.system('python3 portfolio/portfolio.py clean rt')
    os.system('python3 portfolio/portfolio.py build rt --no-stderr')

def deploy_portfolio():
    
    os.chdir('/home/jovyan/data-analyses')
    os.system('python3 portfolio/portfolio.py build rt --no-execute-papermill --deploy')

if __name__ == "__main__":

    speedmaps_index_joined = pd.read_parquet(PROGRESS_PATH)
    make_rt_site_yml(speedmaps_index_joined)
    stage_portfolio()
    deploy_portfolio()