import calitp_data_analysis.magics
import geopandas as gpd
import pandas as pd

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

blue_palette = ["#B9D6DF", "#2EA8CE", "#0B405B"]
red_green_yellow = ["#ec5d3b", "#fde18d", "#7cc665"]