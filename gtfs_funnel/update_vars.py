import yaml
from pathlib import Path
from shared_utils import rt_dates

all_dates = (rt_dates.y2024_dates + rt_dates.y2023_dates + 
             rt_dates.oct_week + rt_dates.apr_week)

analysis_date_list = [
    rt_dates.DATES["jan2024"]
] 

CONFIG_PATH = Path("config.yml")

with open(CONFIG_PATH) as f: 
    CONFIG_DICT = yaml.safe_load(f)  