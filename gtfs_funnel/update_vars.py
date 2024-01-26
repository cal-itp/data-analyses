import yaml
from pathlib import Path
from shared_utils import rt_dates

months = ["dec", "nov", "oct", "sep", "aug",
         "jul", "jun", "may", "apr", "mar"]         

days = ["a", "b", "c", "d", "e", "f"]

analysis_date_list = [
    rt_dates.DATES["jan2024"]
] 

CONFIG_PATH = Path("config.yml")

with open(CONFIG_PATH) as f: 
    CONFIG_DICT = yaml.safe_load(f)  