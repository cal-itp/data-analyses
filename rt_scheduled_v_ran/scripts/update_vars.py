from shared_utils import rt_dates
import yaml
from pathlib import Path

months = ["nov", "dec"]         

analysis_date_list = [
    rt_dates.DATES[f"{m}2023"] for m in months
]
