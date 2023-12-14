import yaml
from pathlib import Path
from shared_utils import rt_dates

months = ["nov", "oct", "sep", "aug",
         "jul", "jun", "may", "apr", "mar"]         

analysis_date_list = [
    rt_dates.DATES["dec2023"] 
]

CONFIG_PATH = Path("config.yml")

with open(CONFIG_PATH) as f: 
    CONFIG_DICT = yaml.safe_load(f)  