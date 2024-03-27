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

# initial df with cpb col
all_bus_cpb = overall_cpb(all_bus)

# get zscore
cpb_zscore = get_zscore(all_bus_cpb)

# initial df with cpb/zscore, remove outliers
no_outliers = remove_outliers(cpb_zscore, "zscore_cost_per_bus")

# aggregate by transit agency
agency_agg = cpb_aggregate(all_bus, "transit_agency")

# aggregate by prop type
prop_agg = cpb_aggregate(all_bus, "prop_type")

# aggregate by bus size
size_agg = cpb_aggregate(all_bus, "bus_size_type")

min_bus_cost = cpb_zscore.cpb.min()
max_bus_cost = cpb_zscore.cpb.max()
max_bus_count = cpb_zscore.bus_count.max()

# VARIABLES
cpb_mean = cpb_zscore.cpb.mean()
cpb_std = cpb_zscore.cpb.std()

# agency with highest bus count
agency_with_most_bus = cpb_zscore.loc[
    cpb_zscore["bus_count"].idxmax(), "transit_agency"
]

# propulsion type max count and name
prop_type_name_max_freq = cpb_zscore["prop_type"].value_counts().idxmax()
prop_type_max = cpb_zscore["prop_type"].value_counts().max()

# prop type min count and anme
prop_type_name_min_freq = cpb_zscore["prop_type"].value_counts().idxmin()
prop_type_min = cpb_zscore["prop_type"].value_counts().min()

# how many buses do they have? already answered
agency_with_highest_funds = cpb_zscore.loc[
    all_bus["total_cost"].idxmax(), "transit_agency"
]

# what is the highest amount? already answered
agency_max_cpb = cpb_zscore.loc[cpb_zscore["cpb"].idxmax(), "transit_agency"]
agency_min_cpb = cpb_zscore.loc[cpb_zscore["cpb"].idxmin(), "transit_agency"]
prop_type_max_cpb = cpb_zscore.loc[cpb_zscore["cpb"].idxmax(), "prop_type"]
prop_type_min_cpb = cpb_zscore.loc[cpb_zscore["cpb"].idxmin(), "prop_type"]

## ZEB ONLY VARIABLES

# zeb only df
zeb_only = zeb_only_df(all_bus)

# calc cpb
zeb_cpb = overall_cpb(zeb_only)

# get zscore
zeb_zscore = get_zscore(zeb_cpb)

# remove outliers
zeb_no_outliers = remove_outliers(zeb_zscore, "zscore_cost_per_bus")

# aggregate by transit agency
zeb_agency_agg = cpb_aggregate(zeb_only, "transit_agency")

# aggregate by prop type
zeb_prop_agg = cpb_aggregate(zeb_only, "prop_type")

# aggregate by bus size
zeb_size_agg = cpb_aggregate(zeb_only, "bus_size_type")

# VARIABLES
zeb_count = len(zeb_only.prop_type)

# zeb only, no outliers cpb curve
zeb_only_mean = zeb_no_outliers.cpb.mean()
zeb_only_std = zeb_no_outliers.cpb.std()

## NON-ZEB VARIABLES

# no zeb df
non_zeb_only = non_zeb_only_df(all_bus)

# calc cpb
non_zeb_cpb = overall_cpb(non_zeb_only)

# get zscore
non_zeb_zscore = get_zscore(non_zeb_cpb)

# remove outliers
non_zeb_no_outliers = remove_outliers(non_zeb_zscore, "zscore_cost_per_bus")

# aggregate by transit agency
non_zeb_agency_agg = cpb_aggregate(non_zeb_only, "transit_agency")

# aggregate by prop type
non_zeb_prop_agg = cpb_aggregate(non_zeb_only, "prop_type")

# aggregate by bus size
non_zeb_size_agg = cpb_aggregate(non_zeb_only, "bus_size_type")

# VARIABLES
non_zeb_count = len(non_zeb_only.prop_type)

# non-zeb cpb mean and std dev
non_zeb_only_mean = non_zeb_no_outliers.cpb.mean()
non_zeb_only_std = non_zeb_no_outliers.cpb.std()

# start summary narative
summary = f"""
## Summary
This analysis examines the 'cost' of buses for transit agencies across the country. Specifically, to exammine the variation of bus cost for different bus related categories such as propulsion type and size type. 

As of today, data was scraped from these sources:
1. FTA Bus and Low- and No-Emission Grant Awards press release (federally funded, nationwide data)
2. TIRCP project data (state-funded, California only)
3. DGS useage report for all procurements from California agencies purchasing from New Flyer and Portera.

Analyzing the dataset uncovered several nuances. Some projects included additional components besides bus purchases. Installing charging inffrastructure, constructing transit facilities, and other non-bus components were often wrapped into bus purchases. While there were projects that purchased buses exclusivly, some projects did not include any bus purchases at all or did not accurately or describe the propulsion or bus size type. The variety in these projects may contribute to high variances in “cost per bus”.

The dataset was examined for inconsistencies and data was validated to complete the analysis. The final Dataset was filtered for projects that only procured buses.The compiled data was aggregated by agencies and a 'cost per bus' metric was calculated by dividing the total funding received by the total number of buses they procured.
Initial finding uncovered some outliers where a transit agency’s cost per bus figure exceeded 3 standard deviations away from the mean.

Overall:
- **{total_unique_projects}** projects with bus purchases were analyzed.
- **{total_funding:,.2f}** dollars were awarded to agencies for projects including bus purchases.
- **{total_bus_count}** total buses are to be purchased.


Propulsion type values varied wildly amongst the datasets and often times did not explicilty specify the propulsion type. Data was validated and grouped as best as possible based on project description or other indications of specific propulsion type.
The following is a summary of propulsion type metrics.
- The most common propulsion type that was proceeded was **"{prop_type_name_max_freq}"**.
- The number of zero-emission buses procured (electric, battery-electric and fuel-cell electric) is **{zeb_count}**.
- the number of non-zero emission buses procured (CNG, hybrids, other alternate fuels) is **{non_zeb_count}**.
     
The following was discovered after removing outliers:
- overall the average awarded dollars per bus is {cpb_mean:,.2f} dollars, with a standard deviation of {cpb_std:,.2f} dollars. 
- the average awarded dollars per ZEB is {zeb_only_mean:,.2f} dollars, with a standard deviation of {zeb_only_std:,.2f} dollars.
- the average awarded dollars per non-ZEB {non_zeb_only_mean:,.2f} dollars, with a standard deviation of ${non_zeb_only_std:,.2f} dollars.

Below are key charts that summarize the findings.

"""

all_bus_desc = """
## All buses (ZEB and non-ZEB) distribution curve.
This chart shows the distribution of all project.
"""

# ZEB only, cpb distribution
zeb_desc = """
## ZEB only  cost/bus Distribution Chart. 
Chart of projects with electric, battery electric, hydrogen fuel cell bus procurements
The majority of the distribution is within +/-1 standard deviation of the mean, however the standard deviation is quite wide at ~$1,200,000.
"""

# non-ZEB
non_zeb_desc = """
## non-ZEB cost/bus Distribution. 
Chart of projects with non-ZEB bus procurement (hybrids, diesel, cng)
This distrubtion is is much more spread out and with a smaller standard deviation."""

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
The total cost per bus for ZEB categories do fall within a similar range of eachother."""

#prop_type bus coutn
bus_count_prop_type_desc = """
## Bus count by propulsion type. 
The most common bus type procured were zero-emissions related."""

conclusion = """
## Conclusion
Based on the findings so far, there is data to supports that bus procurement cost vary widely amongst transit agencies all over the country. 
Non-ZEB bus procurement shows wider cost variation. Where as ZEB procurement was much tighter. ZEBs do have a higher cost per bus than non-ZEB, however this may be due to projects that include ZEBs and other bus related expenses.

Lots of project included procuring buses and other components such as charging equipment/stations, new faciliites and other installation/construction activies. The majority of ZEB procurement projects also include the purchase of related charging equipment and infrastructure, indicating that transit agencies are in the early stages of adopting ZEBs and require the initial investment of charging related equipment to operate the buses. Few agencies reported procuring only ZEB buses.

the majority of bus only procurement projects were for non-ZEB bus types.

Futher investigation is needed to isolate projects with only busy procurement. The DGS usage report data is the most granual data and includes itemized cost for bus, options and other configurables.
"""