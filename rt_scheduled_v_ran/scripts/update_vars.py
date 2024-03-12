from shared_utils import rt_dates
import yaml
from pathlib import Path

trip_months = ["sep", "oct"]         

trip_analysis_date_list = [
    rt_dates.DATES[f"{m}2023"] for m in trip_months
]

oct_week = rt_dates.get_week("oct2023", exclude_wed=True)
apr_week = rt_dates.get_week("apr2023", exclude_wed=True)

route_analysis_date_list = (rt_dates.y2024_dates + 
                            rt_dates.y2023_dates + 
                            oct_week + apr_week)    

CONFIG_PATH = Path("config.yml")

with open(CONFIG_PATH) as f: 
    CONFIG_DICT = yaml.safe_load(f)  