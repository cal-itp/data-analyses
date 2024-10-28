import pandas as pd
import numpy as np
import altair as alt
from calitp_data_analysis import calitp_color_palette
from IPython.display import HTML, Image, Markdown, display, display_html

def reverse_snakecase(df:pd.DataFrame)->pd.DataFrame:
    """
    Clean up columns to remove underscores and spaces.
    """
    df.columns = df.columns.str.replace("_", " ").str.strip().str.title()
    
    df.columns = (df.columns.str.replace("Dac", "DAC")
                  .str.replace("Vmt", "VMT")
                  .str.replace("Zev", "ZEV")
                  .str.replace("Lu", "Landuse")
                  .str.replace("Ct", "CalTrans")
                 )
    return df

def load_dataset()->pd.DataFrame:
    """
    Load the final dataframe.
    """
    GCS_FILE_PATH = "gs://calitp-analytics-data/data-analyses/starter_kit/"
    FILE = "starter_kit_example_categorized.parquet"
    
    # Read dataframe in
    df = pd.read_parquet(f"{GCS_FILE_PATH}{FILE}")
    
    # Titlecase the Scope of Work column again since it is all lowercase
    df["Scope of Work"] = df["Scope of Work"].str.title()
    
    # Clean up the column names
    df = reverse_snakecase(df)
    return df

def aggregate_by_category(df: pd.DataFrame) -> pd.DataFrame:
    """
    Find the median overall score and project cost 
    and total unique projects by category.
    """
    agg1 = (
        df.groupby(["Category"])
        .aggregate(
            {
                "Overall Score": "median",
                "Project Cost": "median",
                "Project Name": "nunique",
            }
        )
        .reset_index()
        .rename(
            columns={
                "Overall Score": "Median Score",
                "Project Cost": "Median Project Cost",
                "Project Name": "Total Projects",
            }
        )
    )
    
    # Format the Cost column properly
    agg1['Median Project Cost'] = agg1['Median Project Cost'].apply(lambda x: '${:,.0f}'.format(x))
    
    return agg1

def wide_to_long(df:pd.DataFrame)->pd.DataFrame:
    """
    Change the dataframe from wide to long based on the project name and
    Caltrans District.
    """
    df2 = pd.melt(
    df,
    id_vars=["CalTrans District","Project Name"],
    value_vars=[
        "Accessibility Score",
        "DAC Accessibility Score",
        "DAC Traffic Impacts Score",
        "Freight Efficiency Score",
        "Freight Sustainability Score",
        "Mode Shift Score",
        "Landuse Natural Resources Score",
        "Safety Score",
        "VMT Score",
        "ZEV Score",
        "Public Engagement Score",
        "Climate Resilience Score",
        "Program Fit Score",
    ])
    
    df2 = df2.rename(columns = {'variable':'Metric',
                                'value':'Score'})
    return df2

def style_df(df: pd.DataFrame):
    """
    Styles a dataframe and displays it.
    """
    display(
        df.style.hide(axis="index")
        .format(precision=0)  # Display only 2 decimal points
        .set_properties(**{
            "background-color": "white",
            "text-align": "center"
        })
    )

def create_metric_chart(df: pd.DataFrame) -> alt.Chart:
    """
    Create a chart that displays metric scores
    for each project.
    """
    # Create dropdown
    metrics_list = df["Metric"].unique().tolist()

    metrics_dropdown = alt.binding_select(
        options=metrics_list,
        name="Metrics: ",
    )
    # Column that controls the bar charts
    xcol_param = alt.selection_point(
        fields=["Metric"], value=metrics_list[0], bind=metrics_dropdown
    )

    chart = (
        alt.Chart(df, title="Metric by Categories")
        .mark_bar(size=20)
        .encode(
            x=alt.X("Score", scale=alt.Scale(domain=[0, 10])),
            y=alt.Y("Project Name"),
            color=alt.Color(
                "Score",
                scale=alt.Scale(
                    range=calitp_color_palette.CALITP_CATEGORY_BRIGHT_COLORS
                ),
            ),
            tooltip=list(df.columns),
        )
        .properties(width=400, height=250)
    )
    
    chart = chart.add_params(xcol_param).transform_filter(xcol_param)
    
    return chart

def create_district_summary(df: pd.DataFrame, caltrans_district: int):
    filtered_df = df.loc[df["CalTrans District"] == caltrans_district].reset_index(
        drop=True
    )
    # Finding the values referenced in the narrative
    median_score = filtered_df["Overall Score"].median()
    total_projects = filtered_df["Project Name"].nunique()
    max_project = filtered_df["Project Cost"].max()
    max_project = f"${max_project:,.2f}"

    # Aggregate the dataframe
    aggregated_df = aggregate_by_category(filtered_df)

    # Change the dataframe from wide to long
    df2 = wide_to_long(filtered_df)

    # Create narrative
    display(
        Markdown(
            f"""<h3>District {caltrans_district}</h3>
        The median score for projects in District 3 is <b>{median_score}</b><br> 
        The total number of projects is <b>{total_projects}</b><br>
        The most expensive project costs <b>{max_project}</b>
        """
        )
    )
    display(
        Markdown(
            f"""<h4>Metrics aggregated by Categories</h4>
        """
        )
    )
    style_df(aggregated_df)

    display(
        Markdown(
            f"""<h4>Overview of Projects</h4>
        """
        )
    )
    style_df(filtered_df[["Project Name", "Overall Score", "Scope Of Work"]])
    display(
        Markdown(
            f"""<h4>Metric Scores by Project</h4>
        """
        )
    )
    display(create_metric_chart(df2))