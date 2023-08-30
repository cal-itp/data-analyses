import pandas as pd
import _utils 

# Geography
from shared_utils import geography_utils
import geopandas as gpd

# Format currency
from babel.numbers import format_currency

"""
Functions specific to this project
"""
# Create a fake scorecard
def create_fake_score_card(df):
    # Subset 
    df2 = df[
    [
        "project_name",
        "increase_peak_person_throughput",
        "reduction_in_peak_period_delay",
        "reduction_in_fatal_and_injury_crashes",
        "reduction_in_injury_rates",
        "increase_access_to_jobs",
        "increase_access_jobs_to_DAC",
        "commercial_dev_developed",
        "tons_of_goods_impacted",
        "improve_air_quality",
        "impact_natural_resources",
        "support_of_trasnportation",
       
    ]]
    
    # Melt
    df2 = pd.melt(
    df2,
    id_vars=["project_name"],
    value_vars=[
        "increase_peak_person_throughput",
        "reduction_in_peak_period_delay",
        "reduction_in_fatal_and_injury_crashes",
        "reduction_in_injury_rates",
        "increase_access_to_jobs",
        "increase_access_jobs_to_DAC",
        "commercial_dev_developed",
        "tons_of_goods_impacted",
        "improve_air_quality",
        "impact_natural_resources",
        "support_of_trasnportation",
    ])
    
    # Remove underscores off of old column names
    df2["variable"] = df2["variable"].str.replace("_", " ").str.title()
    
    # New column with broader Measures
    df2["Category"] = df2["variable"]

    df2["Category"] = df2["Category"].replace(
    {
        "Increase Peak Person Throughput": "Congestion Mitigation",
        "Reduction In Peak Period Delay": "Congestion Mitigation",
        "Reduction In Fatal And Injury Crashes": "Safety",
        "Reduction In Injury Rates": "Safety",
        "Increase Access To Jobs": "Accessibility Increase",
        "Increase Access Jobs To Dac": "Accessibility Increase",
        "Commercial Dev Developed": "Economic Dev.",
        "Tons Of Goods Impacted": "Economic Dev.",
        "Improve Air Quality": "Environment",
        "Impact Natural Resources": "Environment",
        "Support Of Trasnportation": "Land Use",
    })
    
    # Get total scores
    total = (
    df2.groupby(["project_name", "Category"])
    .agg({"value": "sum"})
    .rename(columns={"value": "Total Category Score"})
    .reset_index())
    
    # Merge
    df2 = pd.merge(
    df2, total, how="left", on=["project_name", "Category"])
    
    # Add fake descriptions
    for i in ["Measure Description",
              "Factor Weight",
              "Weighted Factor Value",
              "Category Description",]:
        df2[i] = "Text Here"
    
    # Second subset
    df3 = df[["total_project_cost__$1,000_",
        "total_unfunded_need__$1,000_",
         "project_name",
        "project_description"]]
    
    # Melt
    df3 = pd.melt(
    df3,
    id_vars=["project_name","project_description",],
    value_vars=[
        "total_project_cost__$1,000_",
        "total_unfunded_need__$1,000_",
    ])
    
    # Change names
    df3 = df3.rename(columns = {'variable':'monetary',
                                'values':'monetary   values'})
    
     # Final Merge
    final = pd.merge(
    df2, df3, how="inner",  on = ["project_name"])
    
    # Remove underscores off of old column names
    final["monetary"] = final["monetary"].str.replace("_", "  ").str.title()
    return final
        
"""
Create summary table: returns total projects, total cost,
and money requested by the column of your choice. 
"""
def summarize_by_project_names(df, col_wanted: str):
    """
    df: original dataframe to summarize
    col_wanted: to column to groupby
    """
    df = (
        df.groupby([col_wanted])
        .agg({"Project Name": "count", 
              "Total Project Cost  $1,000": "sum",
              "Total Unfunded Need  $1,000":"sum"})
        .reset_index()
        .sort_values("Project Name", ascending=False)
        .rename(columns={"Project Name": "Total Projects"})
    )

    df = df.reset_index(drop=True)

    # Create a formatted monetary col
    df["Total Project Cost  $1,000"] = df["Total Project Cost  $1,000"].apply(
        lambda x: format_currency(x, currency="USD", locale="en_US")
    )

    # Create a formatted monetary col
    df["Total Unfunded Need  $1,000"] = df["Total Unfunded Need  $1,000"].apply(
        lambda x: format_currency(x, currency="USD", locale="en_US")
    )
    # Clean up column names, remove snakecase
    df = _utils.clean_up_columns(df)

    return df

"""
Concat summary stats of 
parameter  county & parameter district into one dataframe 
"""
def county_district_comparison(df_parameter_county, df_parameter_district):
    # Grab the full district name
    district_full_name = df_parameter_district["district_full_name"][0]

    # Grab the full county name
    county_full_name = df_parameter_county["full_county_name"][0]

    # Create summary table for district
    district =  summarize_by_project_names(df_parameter_district, "primary_mode")

    # Create summary table for county
    county = summarize_by_project_names(df_parameter_county, "primary_mode")

    # Append grand total and keep only that row...doesn't work when I try to do this with a for loop
    district = (
        district.append(district.sum(numeric_only=True), ignore_index=True)
        .tail(1)
        .reset_index(drop=True)
    )
    county = (
        county.append(county.sum(numeric_only=True), ignore_index=True)
        .tail(1)
        .reset_index(drop=True)
    )

    # Concat
    concat1 = pd.concat([district, county]).reset_index(drop=True)

    # Declare a list that is to be converted into a column
    geography = [district_full_name, county_full_name]
    concat1["Geography"] = geography

    # Drop old cols
    concat1 = concat1.drop(
        columns=[
            "Primary Mode",
            "Fake Fund Formatted",
            "Total Project ($1000) Formatted",
        ]
    )

    # Create new formatted monetary cols
    concat1["Total Project ($1000) Formatted"] = concat1[
        "Total Project Cost  $1,000"
    ].apply(lambda x: format_currency(x, currency="USD", locale="en_US"))

    concat1["Fake Fund Formatted"] = concat1["Current Fake Fund Requested"].apply(
        lambda x: format_currency(x, currency="USD", locale="en_US")
    )

    return concat1

# Summarize districts
def summarize_districts(df, col_wanted: str):
    """
    df: original dataframe to summarize
    col_wanted: to column to groupby
    """
    df = (
        df.groupby([col_wanted])
        .agg(
            {
                "Project Name": "count",
                "Total Project Cost  $1,000": "sum",
                "Total Unfunded Need  $1,000": "sum",
            }
        )
        .reset_index()
        .sort_values("Project Name", ascending=False)
        .rename(columns={"Project Name": "Total Projects"})
    )

    df = df.reset_index(drop=True)

    # Clean up column names, remove snakecase
    df =  _utils.clean_up_columns(df)

    return df