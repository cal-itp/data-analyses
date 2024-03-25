from shared_utils import rt_dates
import yaml
from pathlib import Path

oct_week = rt_dates.get_week("oct2023", exclude_wed=True)
apr_week = rt_dates.get_week("apr2023", exclude_wed=True)

analysis_date_list = [rt_dates.DATES["mar2024"]]    

CONFIG_PATH = Path("config.yml")

with open(CONFIG_PATH) as f: 
    CONFIG_DICT = yaml.safe_load(f)  