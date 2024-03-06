import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import seaborn as sns
import shared_utils
from matplotlib.ticker import ScalarFormatter
from scipy.stats import zscore

def overall_cpb(df: pd.DataFrame) -> pd.DataFrame:
    """
    function to calculate cpb on overall dataframe.
    """
    # copy of df
    df1 = df.copy()
    
    # add new column for cost per bus (cpb)
    df1['cpb'] = (df1['total_cost'] / df1['bus_count']).astype("int64")
    
    return df1

def get_zscore(df: pd.DataFrame) -> pd.DataFrame:
    """
    seperate function to calculate zscore.
    """
    # add new column for z-score
    df1 = df.copy()
    
    df1["zscore_cost_per_bus"] = zscore(df1["cpb"])
    
    return df1

def remove_outliers(df: pd.DataFrame, zscore_col: int) -> pd.DataFrame:
    """
    function to remove zscore outliers from data.
    keeps rows with ascore -3>x<3
    
    """
    df1 = df[
        (df[zscore_col] >= -3) & (df[zscore_col] <= 3)
    ]
    return df1

def cpb_aggregate(df: pd.DataFrame, column: str) -> pd.DataFrame:
    """
    function to aggregate compiled data by different categories (transit agency, propulsion type, size type).
    aggregate on columns:
        "project_title"
        "ppno"
        "total_cost"
        "bus_count"
        
    Then, cost per bus is calculated AFTER the aggregation.
    """
    df_agg = (
        df.groupby(column)
        .agg(
            total_project_count=("project_title", "count"),
            total_project_count_ppno=("ppno", "count"),
            total_agg_cost=("total_cost", "sum"),
            total_bus_count=("bus_count", "sum"),
        )
        .reset_index()
    )
    df_agg["cpb"] = (df_agg["total_agg_cost"] / df_agg["total_bus_count"]).astype("int64")
    return df_agg

def zeb_only_df(df: pd.DataFrame) -> pd.DataFrame:
    """
    filters df to only show rows that are zero-emission buses (ZEB).
    """
    zeb_list =[
       'BEB',
        #'CNG',
        'FCEB',
        'electric (not specified)',
        #'ethanol',
        #'low emission (hybrid)',
        #'low emission (propane)',
        #'mix (diesel and gas)',
        #'mix (zero and low emission)',
        #'not specified',
        'zero-emission bus (not specified)' 
    ]
    df1 = df.copy()
    
    df1 = df1[df1["prop_type"].isin(zeb_list)]
    
    return df1

def non_zeb_only_df(df: pd.DataFrame) -> pd.DataFrame:
    non_zeb_list =[
            'CNG',
            'ethanol',
            'low emission (hybrid)',
            'low emission (propane)',
            'mix (diesel and gas)',
            'mix (zero and low emission)',
    ]
    
    df1 = df.copy()
    
    df1 = df1[df1["prop_type"].isin(non_zeb_list)]
    
    return df1

def dist_curve(
    df: pd.DataFrame,
    mean: str,
    std: str,
    title: str,
    xlabel: str,
):
    """
    function to make distribution curve. uses the "cpb" column of the df.
    """
    sns.histplot(df["cpb"], kde=True, color="skyblue", bins=20)
    # mean line
    plt.axvline(
        mean, color="red", linestyle="dashed", linewidth=2, label=f"Mean: ${mean:,.2f}"
    )
    # mean+1std
    plt.axvline(
        mean + std,
        color="green",
        linestyle="dashed",
        linewidth=2,
        label=f"Standard Deviation: ${std:,.2f}",
    )
    plt.axvline(mean - std, color="green", linestyle="dashed", linewidth=2)
    plt.axvline(mean + (std * 2), color="green", linestyle="dashed", linewidth=2)
    plt.axvline(mean + (std * 3), color="green", linestyle="dashed", linewidth=2)

    plt.title(title + " with Mean and Standard Deviation")
    plt.xlabel(xlabel)
    plt.ylabel("Frequency")

    # Turn off scientific notation on x-axis?
    plt.gca().xaxis.set_major_formatter(ScalarFormatter(useMathText=False))

    plt.legend()
    plt.show()

    return

def make_chart(y_col: str, title: str, data: pd.DataFrame, x_col: str):
    """
    function to create chart. sorts values by y_col ascending."""
    
    data.sort_values(by=y_col, ascending=False).head(10).plot(
        x=x_col, y=y_col, kind="bar", color="skyblue"
    )
    plt.title(title)
    plt.xlabel(x_col)
    plt.ylabel(y_col)

    plt.ticklabel_format(style="plain", axis="y")
    plt.show()
