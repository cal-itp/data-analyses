"""
(1a) daily time-series stop df
* % completeness, % accuracy - 2D histogram?
scatterplot is similar, but this groups them
* avg_prediction_error_minutes
layered histogram or several histograms by day_of_week
* avg_prediction_spread_minutes
layered histogram or several histograms by day_of_week
* n_predictions in the 30 min interval

(1b) aggregated day_type stop df
* % completeness, % accuracy by day_type - 2D histogram?
* avg_prediction_error_minutes
* avg_prediction_spread_minutes
* daily_predictions_per_stop
* can we put the above all in a table, pivoted wide so it's weekday/sat/sun?

would be nice to merge in trip grain, and trip grain has a column
for an array of stop_ids, so when we aggregate to route-direction,
we can unpack the stops for that and grab it from the stop grain

(2a)
(2b)
"""
import altair as alt
import folium
import geopandas as gpd
import pandas as pd

def make_layered_histogram(
    df: pd.DataFrame, 
    plot_col:str,
    step_size: float
):
    """
    Might be useful for distributions of the metrics by day_type
    https://altair-viz.github.io/gallery/layered_histogram.html
    
    Make a symmetrical distribution, centered on 0.
    Find the larger of min and max and use that as bounds.
    """
    vertical_line = alt.Chart(
        pd.DataFrame({'line_x': [0]})).mark_rule(
        color='red'
    ).encode(
        x=alt.X('line_x:Q', title="")
    )
    
    chart = (
        alt.Chart(df)
        .mark_bar(
            opacity=0.3,
            binSpacing=0
    ).encode(
        alt.X(f'{plot_col}:Q', bin=alt.Bin(step=step_size), 
             #scale=alt.Scale(domain=[bounds * -1, bounds])
             ),
        alt.Y('count()', stack=None),
        alt.Color(
            'weekday_weekend:N',
            scale=alt.Scale(
                range=["#ccbb44","#5b8efd","#dd217d"]) # tri_color color_palette
            )
        )
    )
    
    return chart + vertical_line


def counts_for_2d_histogram(
    df: pd.DataFrame
) -> pd.DataFrame:
    """
    Group by the combinations of the 2 percent columns.
    Otherwise, each stop is overlaying the other, and we can't actually see.
    """
    df2 = (
        df
        .groupby(["pct_complete_minutes", "pct_accurate_minutes", "weekday_weekend"])
        .agg({"stop_id": "count"})
        .reset_index()
        .rename(columns = {"stop_id": "n_stops"})
    )
    
    return df2
    
def make_2d_histogram(
    df: pd.DataFrame,
    title: str = "",
):
    """
    https://altair-viz.github.io/gallery/histogram_scatterplot.html
    prefer this one: https://altair-viz.github.io/gallery/histogram_heatmap.html
    """
    aggregated_df = counts_for_2d_histogram(df)
    
    chart = (
        alt.Chart(aggregated_df)
        .mark_rect()
        .encode(
            x = alt.X(
                "pct_complete_minutes:Q", 
                bin=alt.Bin(maxbins=20), 
                title = "% complete"
            ),
            y = alt.Y(
                "pct_accurate_minutes:Q", 
                bin=alt.Bin(maxbins=20), 
                title = "% accurate"
            ),
            color=alt.Color(
                'sum(n_stops):Q', 
                scale=alt.Scale(scheme="greenblue")
                #range=["#ccbb44","#5b8efd","#dd217d"] # color_palette
            ), 
            #size='sum(n_stops)',
            tooltip=["weekday_weekend", "n_stops",
                     "pct_complete_minutes", "pct_accurate_minutes"]
        ).properties(title=title).interactive()
    )
    
    return chart
        
def make_boxplot_by_day_type(
    df: pd.DataFrame, 
    plot_col: str,
    title: str
) -> alt.Chart:
    """
    """
    boxplot = (
        alt.Chart(
            df[["weekday_weekend", "service_date", "stop_id", "stop_name", plot_col]]
        ).mark_boxplot(size=4, opacity=0.7)
        .encode(
            x="service_date:T",
            y=f"{plot_col}:Q",
            color=alt.Color(
                'weekday_weekend:N', 
                scale = alt.Scale(range=["#ccbb44","#5b8efd","#dd217d"])
            ),
            tooltip=["stop_id", "stop_name", plot_col]
        ).interactive().properties(title = title)
    )

    return boxplot
   
    
def test_jitter(plot_col):
    plot_col = "avg_prediction_error_minutes"

    subset_gdf = gdf1[["weekday_weekend", "service_date", "stop_id", "stop_name", plot_col]]

    jitter = alt.Chart(subset_gdf).mark_circle(size=8).encode(
        x="service_date:T",
        y=f"{plot_col}:Q",
        xOffset="jitter:Q",
        yOffset="jitter:Q",
        color=alt.Color('weekday_weekend:N', legend=None)
    ).transform_calculate(
        # Generate Gaussian jitter with a Box-Muller transform
        #jitter="sqrt(-2*log(random()))*cos(2*PI*random())"
        jitter='random()'
    )

    return jitter

def stop_map_of_metric(
    gdf: gpd.GeoDataFrame,
    plot_col: str
):
    subset_gdf = gdf[["stop_id", "stop_name", "weekday_weekend", plot_col, "geometry"]]
    
    weekday_df = subset_gdf[subset_gdf.weekday_weekend=="Weekday"] 
    sat_df = subset_gdf[subset_gdf.weekday_weekend=="Saturday"] 
    sun_df = subset_gdf[subset_gdf.weekday_weekend=="Sunday"]
    
    m = weekday_df.explore(
        plot_col,
        tiles = "CartoDB Positron",
        name="Weekday"
    )
    
    if len(sat_df) > 0:
        m = sat_df.explore(plot_col, m=m, name="Saturday", legend=False)
    
    if len(sun_df) > 0:
        m = sun_df.explore(plot_col, m=m, name="Sunday", legend=False)
    
    folium.LayerControl().add_to(m)
    
    return m

def manual_quartiles(
    df: pd.DataFrame
) -> pd.DataFrame:
    """
    For percents, start with 0.25, 0.5, 0.75.
    For other metrics, divide into 4 groups?
    """
    pct_scale_cols = ["pct_accurate_minutes", "pct_complete_minutes"]
    other_metric_cols = ["avg_prediction_error_sec", "avg_prediction_spread_minutes", "avg_predictions_per_trip"]

    for c in pct_scale_cols:
        df[f"{c}_categorized"] = pd.qcut(df[c], q=[0, 0.25, 0.5, 0.75, 1], labels=['< 25%', '25-50%', '50-75%', '> 75%'])
    
    for c in other_metric_cols:
        df[f"{c}_categorized"] = pd.qcut(df[c], q=4)

    return df
        
    
def make_bar_chart(df: pd.DataFrame, plot_col: str): 
    """
    Put a daily chart, x=service_date, y=metric,
    see if we can merge in the avg for the day_type
    """
    chart = (
        alt.Chart(df)
        .mark_bar()
        .encode(
            x=alt.X(
                "yearmonthdate(service_date):O", 
                title="Date", 
                axis=alt.Axis(labelAngle=-45)
            ),
            y=alt.Y(plot_col),
            column=alt.Column("day_type:N")
        )
    )

    return chart

#make_layered_histogram(df, "avg_prediction_spread_minutes")
#make_layered_histogram(df, "avg_prediction_error_sec")
#make_layered_histogram(df, "pct_accurate_minutes")
#make_layered_histogram(df, "n_predictions") # this covers the period that's 30 minutes before arrival


def base_facet_line_chart(
    df: pd.DataFrame,
    y_col: str,
    facet_col: str,
    ruler_col: str
) -> alt.Chart:

    # Create the ruler chart
    ruler = (
            alt.Chart(df)
            .mark_rule(color="red", strokeDash=[10, 7])
            .encode(y=f"{ruler_col}:Q")
        )

    chart = (
            alt.Chart(df)
            .mark_line()
            .encode(
                x=alt.X(
                    "stop_name:O",
                    title="Stop",
                    order = "stop_sequence",
                    axis=alt.Axis(labelAngle=-45),
                ),
            )
        )
    # Add ruler plus main chart
    chart = (chart + ruler).properties(width=200, height=250)
    chart = chart.facet(
            column=alt.Column("{facet_col}:N"),
        ).properties(
            #title={
            #    "text": [title],
            #    "subtitle": [subtitle],
            #}
        )
    
    return chart

