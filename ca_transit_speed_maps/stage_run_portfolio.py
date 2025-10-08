import sys
# https://docs.python.org/3/library/warnings.html
if not sys.warnoptions:
    import warnings
    warnings.simplefilter("ignore")

from tqdm import tqdm
import time

import pandas as pd

import datetime as dt
from shared_utils import rt_utils, portfolio_utils

import os

import pyaml
import yaml
from update_vars_index import PROGRESS_PATH

def stage_portfolio():
    
    os.chdir('/home/jovyan/data-analyses')
    os.system('python3 portfolio/portfolio.py clean rt')
    os.system('python3 portfolio/portfolio.py build rt --no-stderr')

def deploy_portfolio():
    
    os.chdir('/home/jovyan/data-analyses')
    os.system('python3 portfolio/portfolio.py build rt --no-execute-papermill --deploy')

if __name__ == "__main__":

    speedmaps_index_joined = pd.read_parquet(PROGRESS_PATH).sort_values(['caltrans_district', 'analysis_name'])
    portfolio_utils.create_portfolio_yaml_chapters_with_sections(portfolio_site_yaml='../portfolio/sites/rt.yml',
                                                 df=speedmaps_index_joined,
                                                 section_info={'column':'analysis_name', 'name':'analysis_name'}
                                                )
    stage_portfolio()
    deploy_portfolio()