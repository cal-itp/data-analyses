# all functions used for annual ridership report

import pandas as pd

def get_percent_change(
    df: pd.DataFrame, 
) -> pd.DataFrame:
    """
    Calculates % change of UPT from previous year 
    """
    df["pct_change_1yr"] = (
        (df["upt"] - df["previous_y_upt"])
        .divide(df["upt"])
        .round(4)
    )
    
    return df

def add_change_columns(
    df: pd.DataFrame) -> pd.DataFrame:
    """
    Calculates (value) change of UPT from previous year.
    Sorts the df by ntd id, year, mode, service. then shifts the upt value down one row (to the next year,mode,service row) . then adds  new columns: 
        1. previous year/month UPT
        2. change_1yr
    """

    sort_cols2 =  ["ntd_id",
                   "year",
                   "mode", 
                   "service",
                   #"period_month", 
                   #"period_year"
                  ] # got the order correct with ["period_month", "period_year"]! sorted years with grouped months
    
    group_cols2 = ["ntd_id",
                   "mode", 
                   "service"
                  ]
    
    #df[["period_year","period_month"]] = df[["period_year","period_month"]].astype(int)

    df = df.assign(
        previous_y_upt = (df.sort_values(sort_cols2)
                        .groupby(group_cols2)["upt"] 
                        .apply(lambda x: x.shift(1))
                       )
    )

    df["change_1yr"] = (df["upt"] - df["previous_y_upt"])
    
    df = get_percent_change(df)
    
    return df





