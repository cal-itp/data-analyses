import pandas as pd
import numpy as np
from calitp import to_snakecase

from siuba import *

def make_long(df, keep_cols = [], category_name = "managed"):
    df1 = df[["name", "gtfs_schedule_status"] + keep_cols]
    
    for col_name in ["mobility_services", "service_type"]:
        df1 = df1.rename(columns=lambda c: col_name 
                        if c.startswith(col_name) else c)
    
    df1 = df1[["name", "gtfs_schedule_status", "mobility_services", "service_type"]]
    
    
    df2 = df1.assign(
        mobility_services = (df1.mobility_services.fillna("None")
                         .apply(lambda x: x.split(','))
                        )
    )
    
    # Cannot add service_type to explode
    # Service types do not match mobility_services (1 service can provide both flex and fixed route)
    # That's fine, maybe service type is not too important to keep anyway
    df3 = df2.explode("mobility_services")
    
    # New variable to track the category name. 
    # Within mobility services, there are 2 sub-categories, managed / operated
    df3 = df3.assign(
        category = category_name
    )
    
    return df3
    
    