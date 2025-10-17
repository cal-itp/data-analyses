"""
Chart and map functions for report.
"""
import altair as alt
import geopandas as gpd
import pandas as pd

TRI_COLORS = ["#ccbb44","#5b8efd","#dd217d"]
FOUR_COLORS = ["#dd217d","#fcb40e","#ccbb44","#5b8efd"]
FOUR_COLORS2 = ["#ee6677","#66ccee","#ccbb44","#4477aa",]
FULL_CATEGORICAL_COLORS = ["#5b8efd", "#765fec", "#fcb40e", "#fc5c04", "#dd217d", "#ccbb44"]


def histogram_line_chart_by_date(
    df: pd.DataFrame, 
    metric_column: str,
    legend_color_column: str
) -> alt.Chart:
    """
    Distill the results from a histogram (deciles binned) for each day 
    into 1 chart.
    Line chart works better; can't get bar chart to unstack and still select 
    a date for a legend.
    
    Purpose of chart is to show daily differences in distribution, so that we are
    comfortable with moving towards a day_type aggregation (weekday/Sat/Sun summary).
    """
    selection = alt.selection_point(fields=[legend_color_column], bind='legend')
      
    subset_df = df[df.metric==metric_column]
    
    if "Weekday" in subset_df[legend_color_column].unique():
        sort_order = ["Weekday", "Saturday", "Sunday"]
    else:
        sort_order = sorted(subset_df[legend_color_column].unique().tolist())
        
    chart= (
        alt.Chart(subset_df)
        .mark_line(point={"size": 15, "filled": True})
        .encode(
            alt.X('decile_bin'),
            alt.Y('counts:Q'),
            alt.Color(
                f'{legend_color_column}:N', 
                sort = sort_order,
                scale=alt.Scale(range=FULL_CATEGORICAL_COLORS + TRI_COLORS + FOUR_COLORS + FOUR_COLORS2),
                
            ),
            opacity=alt.when(selection).then(alt.value(1)).otherwise(alt.value(0.2)),
            strokeWidth=alt.when(selection).then(alt.value(2)).otherwise(alt.value(1)),
            tooltip = ["decile_bin", "counts", "metric"]
        ).add_params(
            selection
        ).properties(
            title = f"{metric_column.replace('pct_tu', '%').replace('_', ' ')}",
            width = 220, height = 170
        ).interactive()
    )
    
    return chart


def boxplot_by_date(
    df: pd.DataFrame, 
    y_col: str
) -> alt.Chart:
    """
    Get a boxplot for each day to look at distribution avg_prediction_error_minutes
    and avg_prediction_spread_minutes.
    These are more suited to see how "early" or "late" an operator's predictions are.
    If it's centered at 0, that's very on-time/accurate!
    
    Couldn't get alt.datum to work from this:
    https://altair-viz.github.io/user_guide/encodings/index.html#datum-and-value
    """
    df = df.assign(horiz_line = 0)
    
    chart = (
        alt.Chart(df)
        .mark_boxplot()
        .encode(
            x=alt.X('service_date:T', axis=alt.Axis(format="%b %e")),
            y=f'{y_col}:Q',
            color=alt.Color(
                "day_type:N", 
                scale=alt.Scale(
                    domain=["Weekday", "Saturday", "Sunday"], 
                    range=FULL_CATEGORICAL_COLORS)
            ),
        )
    )

    #rule = alt.Chart(df).mark_rule(strokeDash=[2, 2]).encode(
    #    y=alt.datum(0)
    #)
    rule = alt.Chart(df).mark_rule(
        color='black', strokeWidth=1, strokeDash=[2, 2]
    ).encode(
        y='horiz_line'
    )

    combined = (chart + rule).properties(
        title=f"{y_col.replace('_', ' ').title()}"
    )   
    
    return combined
 
    
def bar_chart_by_date(
    df: pd.DataFrame, 
    legend_color_column: str,
    is_stacked: bool
) -> alt.Chart:
    selection = alt.selection_point(fields=[legend_color_column], bind='legend')

    chart = (
        alt.Chart(df)
        .mark_bar(size=20)
        .encode(
            x=alt.X("service_date:T", axis=alt.Axis(format='%b %e')),
            y=alt.Y("count()"),
            color=alt.Color(
                f"{legend_color_column}:N", 
                sort = ["5+ min early", "3-5 min early", "1-3 min early",
                       "1 min early to 1 min late", 
                        "1-3 min late", "5+ min late",
                       "unknown"],
                scale = alt.Scale(range = FULL_CATEGORICAL_COLORS),   
            ),
            opacity=alt.when(selection).then(alt.value(1)).otherwise(alt.value(0.2)),
            tooltip = ["service_date", legend_color_column, "count()"],
            ).add_params(
                selection
        ).properties(
            title = f"{legend_color_column.replace('_', ' ').replace('label', '').title()}",
            width = 350, height = 300
        ).interactive()
    )

    if is_stacked:
        chart = chart.encode(
            y=alt.Y("count()", stack="normalize")
        )

    
    return chart
    

def plot_basic_map(gdf: gpd.GeoDataFrame, plot_col: str, colorscale: str):
    """
    Function for map arguments.
    """
    m = gdf.explore(
        plot_col,
        tiles = "CartoDB Positron", 
        cmap = colorscale, 
        legend=True,
        legend_kwds = {
            "caption": f"{plot_col.replace('pct_tu', '%').replace('_', ' ').title()}"
        }
    )

    return m


def make_map(gdf: gpd.GeoDataFrame, plot_col: str):
    """
    Make map for metric.
    The map gets cluttered with the tooltip, 
    so keep only a small set of columns.
    """    
    keep_cols = [
        "month", "year", "day_type", 
        "stop_id", "stop_name",
        "pct_tu_predictions_early", "pct_tu_predictions_ontime", "pct_tu_predictions_late",
        "avg_prediction_error_minutes", "avg_prediction_spread_minutes",
        "prediction_error_label", "n_predictions",
        "geometry"
    ]
    categorical_cols = ["prediction_error_label"]
    
    if plot_col in categorical_cols:
        colorscale = FULL_CATEGORICAL_COLORS
    else:
        colorscale = "viridis"
    
    
    # try to plot weekday where we can
    if len(gdf[gdf.day_type == "Weekday"]) > 0:
        
        subset_gdf = gdf[
            gdf.day_type == "Weekday"
        ][keep_cols].dropna(subset="geometry").reset_index(drop=True)
        
        m = plot_basic_map(subset_gdf, plot_col, colorscale)
        
        return m
    
    # if there are no weekday rows, then let's plot weekend 
    elif len(gdf[gdf.day_type == "Weekday"]) == 0:
        subset_gdf = gdf[
            gdf.day_type != "Weekday"
        ][keep_cols].dropna(subset="geometry").reset_index(drop=True)
        
        print(f"Weekday map could not be plotted. Plot weekend map.")
        m = plot_basic_map(subset_gdf, plot_col, colorscale)
        
        return m
    
    else:
        print(f"No map could be plotted. Debug error related to schedule + RT data.")


'''
Double check that we have different values for each date

daily_df2[daily_df2.metric=="pct_tu_accurate_minutes"].groupby(
    ["decile_bin"]
).agg({
    "service_date": lambda x: list(x),
    "counts": lambda x: list(x)}
).reset_index()
'''