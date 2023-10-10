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
from rt_analysis import rt_parser
from shared_utils import rt_utils

import os

import pyaml
import yaml
from build_speedmaps_index import ANALYSIS_DATE

def make_rt_site_yml(speedmaps_index_joined,
                       rt_site_path = '../portfolio/sites/rt.yml'):
        
    # make sure intermediate data is ran or at least attempted
    assert speedmaps_index_joined.status.isin(['map_confirmed',
                        'parser_failed', 'map_failed']).all(), 'must run prior scripts first, see Makefile'
    
    with open(rt_site_path) as rt_site:
        rt_site_data = yaml.load(rt_site, yaml.Loader)
    
    chapters_list = []
    speedmaps_index_joined = speedmaps_index_joined >> arrange(_.caltrans_district)
    for district in speedmaps_index_joined.caltrans_district.unique():
        if type(district) == type(None):
            continue
        chapter_dict = {}
        filtered = (speedmaps_index_joined
                    >> filter(_.caltrans_district == district,
                             -_.status.isin(['parser_failed', 'map_failed']))
                    >> arrange(_.organization_name)
                   )
        chapter_dict['caption'] = f'District {district}'
        chapter_dict['params'] = {'district': district}
        chapter_dict['sections'] = \
            [{'itp_id': itp_id} for itp_id in filtered.organization_itp_id.to_list()]
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
    os.system('cp -r portfolio/rt/_build/html/* portfolio/index/rt/')
    os.system('python3 portfolio/portfolio.py build rt --no-execute-papermill --deploy')
    # print('check draft URL for RT site, then run python portfolio/portfolio.py index --deploy')
    # print('after that, check draft URL for index, rerun last with --prod')
    # os.system('netlify deploy --site=cal-itp-data-analyses --dir=portfolio/rt/_build/html/ --alias=rt')

if __name__ == "__main__":

    speedmaps_index_joined = rt_utils.check_intermediate_data(
        analysis_date = ANALYSIS_DATE)
    make_rt_site_yml(speedmaps_index_joined)
    stage_portfolio()
    deploy_portfolio()