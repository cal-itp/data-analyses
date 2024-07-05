import calitp_data_analysis.magics
import geopandas as gpd
import pandas as pd
import yaml

"""
Functions that are used across _section_1_utils and _section_2_utils.
"""
def labeling(word: str) -> str:
    return (
        word.replace("_", " ")
        .title()
        .replace("Pct", "%")
        .replace("Vp", "VP")
        .replace("Route Combined Name", "Route")
        .replace("Ttl", "Total")
    )

with open("readable.yml") as f:
    readable_dict = yaml.safe_load(f)
    
def replace_column_names(column_name):
    if column_name in readable_dict:
        if 'readable' in readable_dict[column_name]:
            return readable_dict[column_name]['readable']
        else:
            return readable_dict[column_name]
    return column_name