"""
Utility functions for CalEnviroScreen data.
"""
import numpy as np
import pandas as pd
import utils

def define_equity_groups(df, percentile_col = ["CIscoreP"], num_groups=5):
    """
    df: pandas.DataFrame
    percentile_col: list.
                    List of columns with values that are percentils, to be
                    grouped into bins.
    num_groups: integer.
                Number of bins, groups. Ex: for quartiles, num_groups=4.
                
    `pd.cut` vs `pd.qcut`: 
    https://stackoverflow.com/questions/30211923/what-is-the-difference-between-pandas-qcut-and-pandas-cut            
    """
    
    for col in percentile_col:
        new_col = f"{col}_group"
        # -999 should be replaced as NaN, so it doesn't throw off the binning of groups
        df[col] = df[col].replace(-999, np.nan)
        df[new_col] = pd.cut(df[col], bins=num_groups, labels=False) + 1

    return df


def prep_calenviroscreen(df):
    # Fix tract ID and calculate pop density
    df = (df.assign(
            Tract = df.Tract.apply(lambda x: '0' + str(x)[:-2]).astype(str),
            sq_mi = df.geometry.area * utils.SQ_MI_PER_SQ_M,
        ).rename(columns = {
            "TotPop19": "Population",
            "ApproxLoc": "City",
        })
    )
    df['pop_sq_mi'] = df.Population / df.sq_mi
    
    df2 = define_equity_groups(
        df,
        percentile_col =  ["CIscoreP", "PolBurdP", "PopCharP"], 
        num_groups = 3)
    
    # Rename columns
    keep_cols = [
        'Tract', 'ZIP', 'Population',
        'sq_mi', 'pop_sq_mi',
        'CIscoreP', 'PolBurdP', 'PopCharP',
        'CIscoreP_group', 'PolBurdP_group', 'PopCharP_group',
        'County', 'City', 'geometry',  
    ]
    
    df3 = (df2[keep_cols]
           .rename(columns = 
                     {"CIscoreP_group": "equity_group",
                     "PolBurdP_group": "pollution_group",
                     "PopCharP_group": "popchar_group",
                     "CIscoreP": "overall_ptile",
                     "PolBurdP": "pollution_ptile",
                     "PopCharP": "popchar_ptile"}
                    )
           .sort_values(by="Tract")
           .reset_index(drop=True)
          )
    
    return df3