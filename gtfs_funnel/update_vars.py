import yaml
from pathlib import Path
from shared_utils import rt_dates

analysis_date_list = [
    rt_dates.DATES["oct2023"],
]

CONFIG_PATH = Path("config.yml")

with open(CONFIG_PATH) as f: 
    CONFIG_DICT = yaml.safe_load(f)  