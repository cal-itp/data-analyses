import os
os.environ["CALITP_BQ_MAX_BYTES"] = str(1_000_000_000_000) ## 1TB?
os.environ['USE_PYGEOS'] = '0'

import pandas as pd
from siuba import *

import datetime as dt
import os
from shared_utils import rt_dates

if __name__ == "__main__":
    
    keys = ['nov2022a', 'nov2022b', 'nov2022c', 'nov2022d']
    dates = [rt_dates.DATES[key] for key in keys]
    for date in dates:
        path = f'./_rt_progress_{date}.parquet'
        if os.path.exists(path):
            df = pd.read_parquet(path)
            df = df >> filter(_.caltrans_district == '04 - Oakland')
            print(f'{path} filtered to d4 only!')
            df.to_parquet(path)
