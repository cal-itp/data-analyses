import os
os.environ["CALITP_BQ_MAX_BYTES"] = str(1_000_000_000_000) ## 1TB?
os.environ['USE_PYGEOS'] = '0'

import pandas as pd
from siuba import *

import datetime as dt
import os

if __name__ == "__main__":
    
    dates = [f'2023-04-{day}' for day in range(10, 14)]
    dates = dates[:2] + [dates[-1]] # apr 12 exists for all operators
    for date in dates:
        path = f'./_rt_progress_{date}.parquet'
        if os.path.exists(path):
            df = pd.read_parquet(path)
            df = df >> filter(_.caltrans_district == '04 - Oakland')
            print(f'{path} filtered to d4 only!')
            df.to_parquet(path)
