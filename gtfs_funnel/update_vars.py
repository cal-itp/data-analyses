from omegaconf import OmegaConf # this is yaml parser
from pathlib import Path
from shared_utils import rt_dates

all_dates = (rt_dates.y2024_dates + rt_dates.y2023_dates + 
             rt_dates.oct_week + rt_dates.apr_week)

analysis_date_list = [
    rt_dates.DATES["mar2024"]
] 

CONFIG_PATH = Path("config.yml")
CONFIG_DICT = OmegaConf.load(Path("config.yml"))