import altair as alt
import pandas as pd

from omegaconf import OmegaConf
import _report_visuals_utils 
readable_dict = OmegaConf.load("readable2.yml")


def data_wrangling_for_visualizing2(df, subset, readable_col_names):
    """
    Depending on how much this is used, some stuff
    might be moved outside to be variables borrowed elsewhere.
    remove the args subset, readable_col_names
    """
    
    # create new columns
    # what is the formatting on this? it should be included...for now, it's in the floats
    df = df.assign(
        headway_in_minutes = 60 / df.frequency
    )
    
    # these show up as floats but should be integers
    # also these aren't kept...
    route_typology_cols = [
        f"is_{c}" for c in 
        ["express", "rapid",
         "ferry", "rail", "coverage",
         "local", "downtown_local"]
    ]
    
    # the pct_ columns are included here....do you want to round it first
    # and then scale it up? i dealt with this by excluding it
    float_cols = [c for c in df.select_dtypes(include=["float"]).columns 
                     if c not in route_typology_cols and "pct" not in c]
    
    df[float_cols] = df[float_cols].round(2)
    
    # these had 3 decimal places, then when it gets scaled, it just has 1 decimal place
    # is that what you want? or you want it rounded to the nearest integer?
    # whatever you decide, it should be obvious bc of the code, not because 
    # of the order of the code execution
    pct_cols = [c for c in df.columns if "pct" in c]
    df[pct_cols] = df[pct_cols] * 100

    
    # subset to schedule and vp / why is this done now? do you publish schedule operators?
    # or do schedule_only operators only get the first section?
    # the subset columns is missing sched_rt_category, and it needs an
    # entry in the rename dict if it's used within text table as combo column?
    
    df2 = df.assign(
        time_period = df.time_period.str.replace("_", " ").str.title()
    )[subset].query(
        'sched_rt_category == "schedule_and_vp"'
    ).rename(
        columns = readable_col_names
    ).reset_index(drop=True)

    return df2


def ruler_chart(df: pd.DataFrame, ruler_value: int) -> pd.DataFrame:
    """
    Modification here is to use .assign so that the red error message
    doesn't pop up. distracting!
    """
    # Add the ruler column
    df = df.assign(
        ruler = ruler_value
    )
    chart = (
        alt.Chart(df)
        .mark_rule(color="red", strokeDash=[10, 7])
        .encode(y="mean(ruler):Q")
    )
    return chart

def bar_chart(
    x_col: str,
    y_col: str,
    color_col: str,
    color_scheme: list,
    tooltip_cols: list,
    date_format: str = "%b %Y",
) -> alt.Chart:
    """
    Modification here is to take out df
    facet charts will want to pass a data=df through,
    so by structuring the chart this way, it retains the 
    bar chart making without specifying a table
    """
    chart = (
        alt.Chart()
        .mark_bar()
        .encode(
            x=alt.X(
                x_col,
                title=x_col,
                axis=alt.Axis(labelAngle=-45, format=date_format),
            ),
            y=alt.Y(y_col, title=y_col),
            color=alt.Color(
                color_col,
                legend=None,
                title=color_col,
                scale=alt.Scale(range=color_scheme),
            ),
            tooltip=tooltip_cols,
        )
    )

    return chart


def grouped_bar_chart(
    df: pd.DataFrame,
    x_col: str,
    y_col: str,
    color_col: str,
    color_scheme: list,
    tooltip_cols: list,
    date_format: str,
    offset_col: str,
) -> alt.Chart:
    """
    Uses the modified bar chart with no df, tries to set df later. Seems to work.
    """
    chart = bar_chart(
        x_col, y_col, color_col, color_scheme, tooltip_cols, date_format
    )
    
    # Add Offset
    chart2 = chart.mark_bar(size=5).encode(
        xOffset=alt.X(offset_col, title=offset_col),
    ).properties(data=df)
    
    return chart2


def sample_spatial_accuracy_chart(df):
    specific_chart_dict = readable_dict.spatial_accuracy_graph

    ruler = ruler_chart(df, 100)

    bar = bar_chart(
        x_col = "Date", 
        y_col = "% VP within Scheduled Shape", 
        color_col = "% VP within Scheduled Shape", 
        color_scheme = [*specific_chart_dict.color], 
        tooltip_cols = [*specific_chart_dict.tooltip], 
        date_format="%b %Y"
    )
   
    # write this way so that the df is inherited by .facet
    chart = alt.layer(bar, ruler, data = df).properties(width=200, height=250)
    chart = chart.facet(
        column=alt.Column(
            "Direction:N",
        )
    ).properties(
        title={
            "text": specific_chart_dict.title,
            "subtitle": specific_chart_dict.subtitle,
        }
    )
    
    return chart


def sample_avg_scheduled_min_chart(df):
    specific_chart_dict = readable_dict.avg_scheduled_min_graph
    
    chart = grouped_bar_chart(
        df,
        x_col = "Date",
        y_col="Average Scheduled Service (trip minutes)",
        color_col="Direction:N",
        color_scheme = [*specific_chart_dict.colors],
        tooltip_cols = [*specific_chart_dict.tooltip],
        date_format = "%b %Y",
        offset_col="Direction:N",
    )
        
    chart = _report_visuals_utils.configure_chart(
        chart,
        width = 400, height = 250, 
        title = specific_chart_dict.title, 
        subtitle = specific_chart_dict.subtitle
    )    
    
    return chart