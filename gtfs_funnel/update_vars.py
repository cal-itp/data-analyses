import yaml
from pathlib import Path
from shared_utils import rt_dates

months = [
    "oct"
]         

analysis_date_list = [
    rt_dates.DATES["oct2023a"],
    rt_dates.DATES["oct2023b"]
]

CONFIG_PATH = Path("config.yml")

with open(CONFIG_PATH) as f: 
    CONFIG_DICT = yaml.safe_load(f)  