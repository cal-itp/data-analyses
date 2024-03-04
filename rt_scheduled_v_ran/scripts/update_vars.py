from shared_utils import rt_dates
import yaml
from pathlib import Path

#trip_months = ["sep", "oct"]         

#trip_analysis_date_list = [
#    rt_dates.DATES[f"{m}2023"] for m in trip_months
#]

trip_analysis_date_list = ['2023-12-13','2024-01-17','2024-02-14']


CONFIG_PATH = Path("config.yml")

with open(CONFIG_PATH) as f: 
    CONFIG_DICT = yaml.safe_load(f)  