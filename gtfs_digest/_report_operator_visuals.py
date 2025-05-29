import pandas as pd
import _report_visuals_utils
import altair as alt
from omegaconf import OmegaConf
readable_dict = OmegaConf.load("readable2.yml")
"""
Reshape data
"""
def reshape_route_typology(df: pd.DataFrame) -> pd.DataFrame:
    """
    Reshape dataframe to display
    route types in a pie chart.
    """
    route_type_cols = [
        "n_downtown_local_routes",
        "n_local_routes",
        "n_coverage_routes",
        "n_rapid_routes",
        "n_express_routes",
        "n_rail_routes",
        "n_ferry_routes",
    ]

    # Subset & transform.
    df2 = df[route_type_cols].T.reset_index()

    # Rename the columns in readable_dict area?
    df2.columns = ["Route Type", "Total Routes"]

    # Clean up values in Route Type
    # Use regex to remove 'n_' prefix and '_routes' suffix, and replace '_' with space
    df2["Route Type"] = (
        df2["Route Type"]
        .str.replace(r"^n_", "", regex=True)
        .str.replace("_", " ")
        .str.title()
    )

    return df2

def reshape_percentile_groups(df: pd.DataFrame) -> pd.DataFrame:
    """
    Total number of routes by each
    the route_length_miles_percentile groups.
    """
    agg1 = (
        df.groupby(["percentile_group", "route_length_miles_percentile"])
        .agg({"route_id": "nunique"})
        .reset_index()
    ).rename(
        columns={"percentile_group": "Percentile Group", "route_id": "Total Routes"}
    )
    return agg1

def reshape_longest_shortest_route(df: pd.DataFrame) -> pd.DataFrame:
    """
    Filter for only the longest and shortest route
    """
    df2 = df.loc[
        (df.shortest_longest == "shortest") | (df.shortest_longest == "longest")
    ][["route_length_miles", "route_id", "recent_combined_name"]]

    df2 = df2.drop_duplicates(subset=["route_length_miles"])

    df2.columns = df2.columns.str.title().str.replace("_", " ")
    return df2

"""
Make Visuals
"""
def route_typology_chart(df) -> alt.Chart:

    # Reshape dataframe from wide to long
    df2 = reshape_route_typology(df)

    specific_chart_dict = readable_dict.route_typology_graph

    chart = _report_visuals_utils.pie_chart(
        df=df2,
        color_col="Route Type",
        theta_col="Total Routes",
        color_scheme=[*specific_chart_dict.colors],
        tooltip_cols=[*specific_chart_dict.tooltip],
    ).properties(width=400, height=250)

    chart = chart.properties(title={"text": specific_chart_dict.title})
    return chart

def percentile_routes_chart(df: pd.DataFrame) -> alt.Chart:

    # Reshape the dataframe
    agg1 = reshape_percentile_groups(df)

    specific_chart_dict = readable_dict.route_percentiles

    chart = _report_visuals_utils.bar_chart(
        x_col="Total Routes",
        y_col="Percentile Group",
        color_col="Percentile Group",
        color_scheme=[*specific_chart_dict.colors],
        tooltip_cols=[*specific_chart_dict.tooltip],
        date_format="",
    )

    chart = alt.layer(chart, data=agg1).properties(width=400, height=150)

    chart = chart.properties(
        title={
            "text": f"{specific_chart_dict.title}",
            "subtitle": specific_chart_dict.subtitle,
        }
    )
    return chart

def shortest_longest_routes_chart(df: pd.DataFrame) -> alt.Chart:

    # Reshape dataframe for graphing
    df2 = reshape_longest_shortest_route(df)

    specific_chart_dict = readable_dict.longest_shortest_route

    chart = _report_visuals_utils.bar_chart(
        x_col="Route Length Miles",
        y_col="Recent Combined Name",
        color_col="Route Length Miles",
        color_scheme=[*specific_chart_dict.colors],
        tooltip_cols=[*specific_chart_dict.tooltip],
        date_format="",
    )

    chart = alt.layer(chart, data=df2).properties(width=400, height=150)

    chart = chart.properties(
        title={
            "text": f"{specific_chart_dict.title}",
            "subtitle": specific_chart_dict.subtitle,
        }
    )
    return chart