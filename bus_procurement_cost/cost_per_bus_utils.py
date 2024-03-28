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

def cpb_zscore_outliers(df: pd.DataFrame) -> pd.DataFrame:
    """
    function that calculated cost per bus col, z-score col, then removes outliers(remove rows with zscore >3)
    """
    df = overall_cpb(df)
    df = get_zscore(df)
    df1 = remove_outliers(df, "zscore_cost_per_bus")
    
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

### variables

#INITIAL DF AGG VARIABLES

# initial, overall df
all_bus = pd.read_parquet(
    "gs://calitp-analytics-data/data-analyses/bus_procurement_cost/cpb_analysis_data_merge.parquet"
)

# Variables
total_unique_projects = len(all_bus)
total_bus_count = sum(all_bus.bus_count)
total_funding = sum(all_bus.total_cost)

## ALL BUS

# initial df with cpb col
#all_bus_cpb = overall_cpb(all_bus)

# get zscore
#cpb_zscore = get_zscore(all_bus_cpb)

# initial df with cpb/zscore, remove outliers
no_outliers = cpb_zscore_outliers(all_bus)


# aggregate by transit agency
agency_agg = cpb_aggregate(no_outliers, "transit_agency")

# aggregate by prop type
prop_agg = cpb_aggregate(no_outliers, "prop_type")

# aggregate by bus size
size_agg = cpb_aggregate(no_outliers, "bus_size_type")

min_bus_cost = no_outliers.cpb.min()
max_bus_cost = no_outliers.cpb.max()
max_bus_count = no_outliers.bus_count.max()

# VARIABLES
cpb_mean = no_outliers.cpb.mean()
cpb_std = no_outliers.cpb.std()

# agency with highest bus count
agency_with_most_bus = no_outliers.loc[
    no_outliers["bus_count"].idxmax(), "transit_agency"
]

# propulsion type max count and name
prop_type_name_max_freq = no_outliers["prop_type"].value_counts().idxmax()
prop_type_max = no_outliers["prop_type"].value_counts().max()

# prop type min count and anme
prop_type_name_min_freq = no_outliers["prop_type"].value_counts().idxmin()
prop_type_min = no_outliers["prop_type"].value_counts().min()

# how many buses do they have? already answered
agency_with_highest_funds = no_outliers.loc[
    all_bus["total_cost"].idxmax(), "transit_agency"
]

# what is the highest amount? already answered
agency_max_cpb = no_outliers.loc[no_outliers["cpb"].idxmax(), "transit_agency"]
agency_min_cpb = no_outliers.loc[no_outliers["cpb"].idxmin(), "transit_agency"]
prop_type_max_cpb = no_outliers.loc[no_outliers["cpb"].idxmax(), "prop_type"]
prop_type_min_cpb = no_outliers.loc[no_outliers["cpb"].idxmin(), "prop_type"]

## ZEB ONLY VARIABLES

# zeb only df
zeb_only = zeb_only_df(all_bus)

# calc cpb
#zeb_cpb = overall_cpb(zeb_only)

# get cpb, zscore, remove outliers
zeb_no_outliers = cpb_zscore_outliers(zeb_only)

# remove outliers
#zeb_no_outliers = remove_outliers(zeb_zscore, "zscore_cost_per_bus")

# aggregate by transit agency
zeb_agency_agg = cpb_aggregate(zeb_no_outliers, "transit_agency")

# aggregate by prop type
zeb_prop_agg = cpb_aggregate(zeb_no_outliers, "prop_type")

# aggregate by bus size
zeb_size_agg = cpb_aggregate(zeb_no_outliers, "bus_size_type")

# VARIABLES
zeb_count = len(zeb_no_outliers.prop_type)

# zeb only, no outliers cpb curve
zeb_only_mean = zeb_no_outliers.cpb.mean()
zeb_only_std = zeb_no_outliers.cpb.std()

## NON-ZEB VARIABLES

# no zeb df
non_zeb_only = non_zeb_only_df(all_bus)

# calc cpb
#non_zeb_cpb = overall_cpb(non_zeb_only)

# get zscore
#non_zeb_zscore = get_zscore(non_zeb_cpb)

# get cpb, zscore, remove outliers
non_zeb_no_outliers = cpb_zscore_outliers(non_zeb_only)

# aggregate by transit agency
non_zeb_agency_agg = cpb_aggregate(non_zeb_no_outliers, "transit_agency")

# aggregate by prop type
non_zeb_prop_agg = cpb_aggregate(non_zeb_no_outliers, "prop_type")

# aggregate by bus size
non_zeb_size_agg = cpb_aggregate(non_zeb_no_outliers, "bus_size_type")

# VARIABLES
non_zeb_count = len(non_zeb_no_outliers.prop_type)

# non-zeb cpb mean and std dev
non_zeb_only_mean = non_zeb_no_outliers.cpb.mean()
non_zeb_only_std = non_zeb_no_outliers.cpb.std()

# start summary narative
summary = f"""
## Summary
This analysis examines the cost of buses for transit agencies across the county. Specifically, to observe the variation of bus cost for propulsion type and bus sizes.

Data was compiled from three data sources:

1. FTA Bus and Low- and No-Emission Grant Awards press release (federally funded, nationwide data)
2. TIRCP project data (state-funded, California only)
3. DGS usage report for all procurements from California agencies purchasing from New Flyer and Portera Inc..

The compiled dataset includes 298 total transit related projects. However, the initial dataset included projects that encompassed bus procurement and other components such as charging installation and facility construction, as well as non-bus related projects (ferries, trains). The dataset was filtered to exclude projects that were not bus related, indicated 0 buses procured, and projects that contained construction/installation work. 94 projects remained that specified the number of buses to procure and explicitly described procuring buses (bus only projects).

The remaining bus only projects were categorized into different propulsion types and bus sizes, a “cost per bus” value was calculated, and outliers removed.

A overall summary is provided below:
- Total projects: **298**
- Number of projects with mix bus procurement and other components, also non-bus projects: **204** 
- Number of bus only projects: **{total_unique_projects}**
- Total dollars awarded to bus only projects: **`${total_funding:,.2f}`**
- Total number of buses: **{total_bus_count}**
- Most common propulsion type procured for bus only projects: **{prop_type_name_max_freq}** at **{prop_type_max}** projects
- Number of ZEB buses* procured: **{zeb_count}**
- Number of non-ZEB buses** procured: **{non_zeb_count}**
- Overall average cost per bus (ZEB & non-ZEB) is `${cpb_mean:,.2f}` (std `${cpb_std:,.2f}`)
- ZEB average cost per bus is `${zeb_only_mean:,.2f}` (std `${zeb_only_std:,.2f}`)
- Non-ZEB average cost per bus is `${non_zeb_only_mean:,.2f}` (std `${non_zeb_only_std:,.2f}`) 

`*`ZEB buses include: zero-emission (not specified), electric (not specified), battery electric, fuel cell electric

`**`Non-ZEB buses include: CNG, ethanol, low emission (hybrid, propane), diesel, gas.


Below are key charts that visualize more findings:


"""

all_bus_desc = """
## All buses (ZEB and non-ZEB) cost/bus distribution curve.
This chart shows the cost per bus distribution of all bus only projects.
"""

# ZEB only, cpb distribution
zeb_desc = """
## ZEB only  cost/bus Distribution Chart. 
Chart of projects with zero-emission, electric, battery electric, hydrogen fuel cell bus procurements.
"""

# non-ZEB
non_zeb_desc = """
## non-ZEB cost/bus Distribution. 
Chart of projects with non-ZEB bus procurement (hybrids, diesel, cng)
This distrubtion is wider than the ZEB projects."""

#highest cpb agency
highest_cpb_desc = """
## Highest Cost per Bus by Transit Agency
SFMTA is the agency with the highest cost per bus of all agencies in the analysis
"""

# Highest awarded agency
highest_award = """
## Most funds Awarded by Transit Agency
LA Metro was awarded almost double the next agency. Followed by SFMTA"""

# most buses
most_bus = """
## Highest Bus Count by Agency. 
LA Metro plans to procure the most buses."""

#prop_type cpb
cpb_prop_type_desc = """
## Cost per bus by propulsion type. 
"""

#prop_type bus coutn
bus_count_prop_type_desc = """
## Bus count by propulsion type. 
"""

conclusion = """
## Conclusion
Based on the findings so far in bus only projects, there is evidence that bus procurement cost vary widely amongst transit agencies all over the country. Non-ZEB bus cost variation was wide. Whereas ZEB cost variation was much tighter. However ZEBs do have a higher cost per bus than non-ZEB.

Most of the bus only projects were for non-ZEBs. This can be explained by looking into the initial project list. Lots of projects that procured ZEBs also included the installation of chargers and related charging infrastructure. Indicating that transit agencies are still adopting and preparing for ZEBs and need to make the initial investment in the equipment.  

"""