import calitp_data_analysis.magics
import geopandas as gpd
import pandas as pd
import yaml
"""
Charts
"""
def reverse_snakecase(df):
    """
    Clean up columns to remove underscores and spaces.
    """
    df.columns = df.columns.str.replace("_", " ").str.strip().str.title()
    return df

def labeling(word: str) -> str:
    return (
        word.replace("_", " ")
        .title()
        .replace("Pct", "%")
        .replace("Vp", "VP")
        .replace("Route Combined Name", "Route")
        .replace("Ttl", "Total")
    )

red_green_yellow = ["#ec5d3b", "#fde18d", "#7cc665"]

# Good for reversing so that red is for higher values, green for lower
green_red_yellow = ["#7cc665","#fde18d", "#ec5d3b", ]

# 6 colors: Red, orange, yellow, green, light blue, dark blue
service_hour_scale = ["#7cc665", "#fcaa5f", "#fde18d", "#ec5d3b", "#3b56a4", "#49a2b2"]

# Less garish colors
section1 = ["#fde18d","#49a2b2"]
"""
Yaml
"""
with open("readable.yml") as f:
    readable_dict = yaml.safe_load(f)
    
def replace_column_names(column_name):
    if column_name in readable_dict:
        if 'readable' in readable_dict[column_name]:
            return readable_dict[column_name]['readable']
        else:
            return readable_dict[column_name]
    return column_name