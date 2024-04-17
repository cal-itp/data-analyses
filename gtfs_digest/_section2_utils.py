import calitp_data_analysis.magics
import geopandas as gpd
import pandas as pd

# Charts
from calitp_data_analysis import calitp_color_palette as cp
import altair as alt
alt.data_transformers.enable('default', max_rows=None)

# Great Tables
import great_tables as gt
from great_tables import md

# Display
from IPython.display import HTML, Markdown, display

# Other
from segment_speed_utils.project_vars import RT_SCHED_GCS, SCHED_GCS
from shared_utils import catalog_utils, rt_dates, rt_utils
GTFS_DATA_DICT = catalog_utils.get_catalog("gtfs_analytics_data")

import _report_utils
"""
Schedule_vp_metrics
Functions
"""
def timeliness_tags(row):
    if row.rt_sched_journey_ratio < 1:
        return "Early"
    elif row.rt_sched_journey_ratio < 1.1:
        return "On Time"
    elif 1.1 <= row.rt_sched_journey_ratio < 1.26:
        return "Late by 1-25% of the scheduled time"
    elif 1.26 <= row.rt_sched_journey_ratio < 1.51:
        return "Late by 26-50% of the scheduled time"
    elif 1.51 <= row.rt_sched_journey_ratio:
        return "Late by 50%+ of the scheduled time"
    else:
        return "No Info"
    
def frequency_tags(row):
    if row.frequency < 2:
        return "<1 trip/hour"
    elif 1 <= row.frequency < 2:
        return "1 trip/hour"
    elif 2 <= row.frequency < 3:
        return "2 trips/hour"
    elif 3 <= row.frequency:
        return "3+ trips/hour"
    else:
        return "No Info"
    
    
def vp_per_min_tag(row):
    if row.vp_per_minute < 1:
        return "<1 ping/minute"
    elif 1 <= row.vp_per_minute < 2:
        return "<3 pings/minute"
    elif 2 <= row.vp_per_minute < 3:
        return "<3 pings/minute"
    elif 3 <= row.vp_per_minute:
        return "3+ pings per minute (target)"
    else:
        return "No Info"
    
def add_categories(df:pd.DataFrame) -> pd.DataFrame:
    df["rt_sched_journey_ratio_cat"] = df.apply(timeliness_tags, axis=1)
    df["frequency_cat"] = df.apply(frequency_tags, axis=1)
    df["vp_per_minute_cat"] = df.apply(vp_per_min_tag, axis=1)    
    
    return df

def load_schedule_vp_metrics(name:str)->pd.DataFrame:
    schd_vp_url = f"{GTFS_DATA_DICT.digest_tables.dir}{GTFS_DATA_DICT.digest_tables.route_schedule_vp}.parquet"
    
    df = pd.read_parquet(schd_vp_url, filters=[[("name", "==", name)]])
    
    # Categorize
    df = add_categories(df)
    
    # Round float columns
    float_columns = df.select_dtypes(include=['float'])
    for i in float_columns:
        df[i] = df[i].round(2)
    
    pct_cols = df.columns[df.columns.str.contains("pct")].tolist()
    for i in pct_cols:
        df[i] = df[i] * 100
        
    # Add rulers
    df["ruler_100_pct"] = 100
    df["ruler_for_vp_per_min"] = 2
    return df 

def route_stats(df: pd.DataFrame) -> pd.DataFrame:
    most_recent_date = df.service_date.max()
    route_merge_cols = ["route_combined_name", "direction_id"]

    all_day_stats = df[
        (df.service_date == most_recent_date) & (df.time_period == "all_day")
    ][
        route_merge_cols
        + [
            "avg_scheduled_service_minutes",
            "avg_stop_miles",
            "n_scheduled_trips",
            "sched_rt_category",
        ]
    ]

    peak_stats = df[(df.service_date == most_recent_date) & (df.time_period == "peak")][
        route_merge_cols + ["speed_mph", "n_scheduled_trips", "frequency"]
    ].rename(
        columns={
            "speed_mph": "peak_avg_speed",
            "n_scheduled_trips": "peak_scheduled_trips",
            "frequency": "peak_hourly_freq",
        }
    )

    offpeak_stats = df[
        (df.service_date == most_recent_date) & (df.time_period == "offpeak")
    ][route_merge_cols + ["speed_mph", "n_scheduled_trips", "frequency"]].rename(
        columns={
            "speed_mph": "offpeak_avg_speed",
            "n_scheduled_trips": "offpeak_scheduled_trips",
            "frequency": "offpeak_hourly_freq",
        }
    )

    table_df = (
        pd.merge(
            all_day_stats,
            peak_stats,
            on=route_merge_cols,
            how = "outer"
        )
        .merge(offpeak_stats, on=route_merge_cols, how = "outer")
        .sort_values(["route_combined_name", "direction_id"])
        .reset_index(drop=True)
    )

    numeric_cols = table_df.select_dtypes(include="number").columns
    table_df[numeric_cols] = table_df[numeric_cols].fillna(0)

    return table_df

def timeliness_trips(df: pd.DataFrame):
    to_keep = [
        "service_date",
        "organization_name",
        "direction_id",
        "time_period",
        "route_combined_name",
        "is_early",
        "is_ontime",
        "is_late",
        "n_vp_trips",
    ]
    df = df[to_keep]
    df2 = df.loc[df.time_period != "all_day"].reset_index(drop=True)

    melted_df = df2.melt(
        id_vars=[
            "service_date",
            "organization_name",
            "route_combined_name",
            "time_period",
            "direction_id",
        ],
        value_vars=["is_early", "is_ontime", "is_late"],
    )
    return melted_df

def pct_vp_journey(df: pd.DataFrame, col1: str, col2: str) -> pd.DataFrame:
    to_keep = [
        "service_date",
        "organization_name",
        "direction_id",
        col1,
        col2,
        "route_combined_name",
        "time_period",
        "route_id",
        "ruler_100_pct",
    ]
    df2 = df[to_keep]

    df3 = df2.melt(
        id_vars=[
            "service_date",
            "organization_name",
            "route_combined_name",
            "direction_id",
            "time_period",
            "route_id",
            "ruler_100_pct",
        ],
        value_vars=[col1, col2],
    )

    return df3

"""
Operator Level
"""
def trips_by_gtfs(df):
    by_date_category = (
    pd.crosstab(
        df.service_date,
        df.sched_rt_category,
        values=df.n_scheduled_trips,
        aggfunc="sum",
    )
    .reset_index()
    .fillna(0))
    
    display(gt.GT(by_date_category, rowname_col="service_date")
    .tab_header(
        title="Daily Trips by GTFS Availability",
        subtitle="Schedule only indicates the trip(s) were found only in schedule data. Vehicle Positions (VP) only indicates the trip(s) were found only in real-time data.",
    )
    .cols_label(
        schedule_only="Schedule Only",
        vp_only="VP Only",
        schedule_and_vp="Schedule and VP",
    )
    .fmt_integer(["schedule_only", "vp_only", "schedule_and_vp"])
    .tab_options(container_width="75%")
    .tab_options(table_font_size="12px"))
    
"""
operator_schedule_rt_category
"""
def load_operator_schedule_rt_category(schedule_gtfs_key: list) -> pd.DataFrame:
    df = pd.read_parquet(
        f"{RT_SCHED_GCS}digest/operator_schedule_rt_category.parquet",
        filters=[[("schedule_gtfs_dataset_key", "in", schedule_gtfs_key)]],
    )
    df.n_trips = df.n_trips.astype(int).fillna(0)
    return df


"""
Charts
"""
def create_data_unavailable_chart():
    data = pd.DataFrame({"text": ["Chart unavailable, not enough data."]})

    # Create a text chart using Altair
    chart = (
        alt.Chart(data)
        .mark_text(
            align="center",
            baseline="middle",
            fontSize=12,
            text="Chart unavailable due to lack of data",
        )
        .properties(width=500, height=100)
    )

    return chart

def clean_data_charts(df:pd.DataFrame, y_col:str)->pd.DataFrame:
    df = df.assign(
            time_period=df.time_period.str.replace("_", " ").str.title()
        ).reset_index(drop=True)

    df[y_col] = df[y_col].fillna(0).astype(int)
    df[f"{y_col}_str"] = df[y_col].astype(str)
    

    return df

def grouped_bar_chart(
    df: pd.DataFrame,
    color_col: str,
    y_col: str,
    offset_col: str,
    title: str,
    subtitle: str,
):
    tooltip_cols = [
        "direction_id",
        "time_period",
        "route_combined_name",
        "organization_name",
        "service_date",
        color_col,
        y_col,
    ]

    if len(df) == 0:
        text_chart = create_data_unavailable_chart()
        return text_chart
    else:
        df = clean_data_charts(df,y_col)
        chart = (
            alt.Chart(df)
            .mark_bar(size=10)
            .encode(
                x=alt.X(
                    "yearmonthdate(service_date):O",
                    title=["Grouped by Direction ID", "Date"],
                    axis=alt.Axis(labelAngle=-45, format="%b %Y"),
                ),
                y=alt.Y(f"{y_col}:Q", title=_report_utils.labeling(y_col)),
                xOffset=alt.X(f"{offset_col}:N", title=_report_utils.labeling(offset_col)),
                color=alt.Color(
                    f"{color_col}:N",
                    title=_report_utils.labeling(color_col),
                    scale=alt.Scale(
                        range=_report_utils.red_green_yellow,
                    ),
                ),
                tooltip=tooltip_cols,
            )
        )
        chart = (chart).properties(
            title={
                "text": [title],
                "subtitle": [subtitle],
            },
            width=500,
            height=300,
        )

        return chart
    
def base_facet_line(
    df: pd.DataFrame, y_col: str, title: str, subtitle: str
) -> alt.Chart:
    if len(df) == 0:
        text_chart = create_data_unavailable_chart()
        return text_chart
    else:
        selection = alt.selection_point(fields=['time_period'], bind='legend')

        df = clean_data_charts(df,y_col)
        tooltip_cols = [
            "route_combined_name",
            "route_id",
            "direction_id",
            "time_period",
            f"{y_col}_str",
        ]
        if "pct" in y_col:
            max_y = 100
        elif "per_minute" in y_col:
            max_y = round(df[y_col].max())
        else:
            max_y = round(df[y_col].max(), -1) + 5
        chart = (
            alt.Chart(df)
            .mark_line(size=5)
            .encode(
                x=alt.X(
                    "yearmonthdate(service_date):O",
                    title="Date",
                    axis=alt.Axis(labelAngle=-45, format="%b %Y"),
                ),
                y=alt.Y(
                    f"{y_col}:Q",
                    title=_report_utils.labeling(y_col),
                    scale=alt.Scale(domain=[0, max_y]),
                ),
                color=alt.Color(
                    "time_period:N",
                    title=_report_utils.labeling("time_period"),
                    scale=alt.Scale(range=_report_utils.red_green_yellow),
                ),
                
                strokeWidth=alt.condition(
                    "datum.time_peak == 'All Day'",
                    alt.value(10),
                    alt.value(1)),
            
            tooltip=tooltip_cols,
            )
        )

        chart = chart.properties(width=250, height=300)
        chart = chart.facet(
            column=alt.Column("direction_id:N", title=_report_utils.labeling("direction_id")),
        ).properties(
            title={
                "text": [title],
                "subtitle": [subtitle],
            }
        ).add_params(selection)
        return chart
def base_facet_circle(
    df: pd.DataFrame, y_col: str, ruler_col: str, title: str, subtitle: str
) -> alt.Chart:

    tooltip_cols = [
        "direction_id",
        "time_period",
        "route_combined_name",
        "service_date",
        f"{y_col}_str",
        "variable",
    ]

    if len(df) == 0:
        text_chart = create_data_unavailable_chart()
        return text_chart
    else:
        if "pct" in y_col:
            max_y = 100
        elif "per_minute" in y_col:
            max_y = round(df[y_col].max())
        else:
            max_y = round(df[y_col].max(), -1) + 5
        df = clean_data_charts(df,y_col)
        df = df.assign(
            variable=df.variable.str.replace("_", " ").str.title(),
        ).reset_index(drop=True)
        ruler = (
            alt.Chart(df)
            .mark_rule(color="red", strokeDash=[10, 7])
            .encode(y=f"ruler_100_pct:Q")
        )

        chart = (
            alt.Chart(df)
            .mark_circle(size=100)
            .encode(
                x=alt.X(
                    "yearmonthdate(service_date):O",
                    title="Date",
                    axis=alt.Axis(labelAngle=-45, format="%b %Y"),
                ),
                y=alt.Y(
                    f"{y_col}:Q",
                    title=_report_utils.labeling(y_col),
                    scale=alt.Scale(domain=[0, max_y]),
                ),
                color=alt.Color(
                    "variable:N",
                    title=_report_utils.labeling("variable"),
                    scale=alt.Scale(range=_report_utils.red_green_yellow),
                ),
                tooltip=tooltip_cols,
            )
        )

        chart = chart + ruler
        chart = chart.facet(
            column=alt.Column("direction_id:N", title=_report_utils.labeling("direction_id")),
        ).properties(
            title={
                "text": [title],
                "subtitle": [subtitle],
            }
        )
        return chart    
def base_facet_chart(
    df: pd.DataFrame,
    y_col: str,
    color_col: str,
    facet_col: str,
    title: str,
    subtitle: str,
):
    tooltip_cols = [
        "direction_id",
        "time_period",
        "route_combined_name",
        "organization_name",
        "service_date",
        y_col,
        color_col,
    ]

    if len(df) == 0:
        text_chart = create_data_unavailable_chart()
        return text_chart
    else:
        if "pct" in y_col:
            max_y = 100
        elif "per_minute" in y_col:
            max_y = round(df[y_col].max())
        else:
            max_y = round(df[y_col].max(), -1) + 5
        df = clean_data_charts(df,y_col)
        chart = (
            (
                alt.Chart(df)
                .mark_bar(size=15, clip=True)
                .encode(
                    x=alt.X(
                        "yearmonthdate(service_date):O",
                        title=["Service Date"],
                        axis=alt.Axis(labelAngle=-45, format="%b %Y"),
                    ),
                    y=alt.Y(
                        f"{y_col}:Q",
                        title=_report_utils.labeling(y_col),
                        scale=alt.Scale(domain=[0, max_y]),
                    ),
                    color=alt.Color(
                        f"{color_col}:N",
                        title=_report_utils.labeling(color_col),
                        scale=alt.Scale(range=_report_utils.red_green_yellow),
                    ),
                    tooltip=tooltip_cols,
                )
            )
            .facet(
                column=alt.Column(
                    f"{facet_col}:N",
                )
            )
            .properties(
                title={
                    "text": title,
                    "subtitle": subtitle,
                }
            )
        )
        return chart
    
def base_facet_with_ruler_chart(
    df: pd.DataFrame, y_col: str, ruler_col: str, title: str, subtitle: str
):
    tooltip_cols = [
        "direction_id",
        "time_period",
        "route_combined_name",
        "organization_name",
        "service_date",
        y_col,
    ]

    if len(df) == 0:
        text_chart = create_data_unavailable_chart()
        return text_chart
    else:
        df = clean_data_charts(df,y_col)
        if "pct" in y_col:
            max_y = 100
        elif "per_minute" in y_col:
            max_y = round(df[y_col].max()) + 2
        else:
            max_y = round(df[y_col].max(), -1) + 5
        ruler = (
            alt.Chart(df)
            .mark_rule(color="red", strokeDash=[10, 7])
            .encode(y=f"mean({ruler_col}):Q")
        )
        chart = (
            alt.Chart(df)
            .mark_bar(size=15, clip=True)
            .encode(
                x=alt.X(
                    "yearmonthdate(service_date):O",
                    title=["Service Date"],
                    axis=alt.Axis(labelAngle=-45, format="%b %Y"),
                ),
                y=alt.Y(
                    f"{y_col}:Q",
                    title=_report_utils.labeling(y_col),
                    scale=alt.Scale(domain=[0, max_y]),
                ),
                color=alt.Color(
                    f"{y_col}:Q",
                    title=_report_utils.labeling(y_col),
                    scale=alt.Scale(range=_report_utils.red_green_yellow),
                ),
                tooltip=df[tooltip_cols].columns.tolist(),
            )
        )

        chart = chart + ruler
        chart = chart.facet(column=alt.Column("direction_id:N",)).properties(
            title={
                "text": title,
                "subtitle": [subtitle],
            }
        )

        return chart

def create_text_table(df: pd.DataFrame, direction_id: str):

    df = (
        df.loc[df.direction_id == direction_id].drop_duplicates().reset_index(drop=True)
    )

    if len(df) == 0:
        text_chart = create_data_unavailable_chart()
        return text_chart

    else:
        df2 = df.melt(
            id_vars=[
                "route_combined_name",
                "direction_id",
            ],
            value_vars=[
                "avg_scheduled_service_minutes",
                "avg_stop_miles",
                "n_scheduled_trips",
                "sched_rt_category",
                "peak_avg_speed",
                "peak_scheduled_trips",
                "peak_hourly_freq",
                "offpeak_avg_speed",
                "offpeak_scheduled_trips",
                "offpeak_hourly_freq",
            ],
        )
        # Create a decoy column to center all the text
        df2["Zero"] = 0

        df2.variable = df2.variable.str.replace("_", " ").str.title()
        df2 = df2.sort_values(by=["direction_id"]).reset_index(drop=True)
        df2["combo_col"] = df2.variable.astype(str) + ": " + df2.value.astype(str)
        text_chart = (
            alt.Chart(df2)
            .mark_text()
            .encode(x=alt.X("Zero:Q", axis=None), y=alt.Y("combo_col", axis=None))
        )

        text_chart = text_chart.encode(text="combo_col:N").properties(
            title=f"Route Statistics for Direction {direction_id}",
            width=500,
            height=300,
        )
        return text_chart

def frequency_chart(df: pd.DataFrame):
    if len(df) == 0:
        text_chart = create_data_unavailable_chart()
        return text_chart

    else:
        chart = (
            alt.Chart(df, width=180, height=alt.Step(10))
            .mark_bar()
            .encode(
                alt.Y(
                    "yearmonthdate(service_date):O",
                    title="Date",
                    axis=alt.Axis(format="%b %Y"),
                ),
                alt.X("frequency:Q", title=_report_utils.labeling("frequency"), axis=None),
                alt.Color("frequency", scale=alt.Scale(range=_report_utils.red_green_yellow)).title(
                    _report_utils.labeling("Frequency")
                ),
                alt.Row("time_period:N")
                .title(_report_utils.labeling("time_period"))
                .header(labelAngle=0),
                alt.Column("direction_id:N").title(_report_utils.labeling("direction_id")),
            )
        )

        chart = chart.properties(title="Frequency of Trips per Hour")
        return chart
"""
Route-Direction
Section
"""
def filtered_route(
    df: pd.DataFrame,
) -> alt.Chart:
    """
    https://stackoverflow.com/questions/58919888/multiple-selections-in-altair
    """
    routes_list = df["route_combined_name"].unique().tolist()


    route_dropdown = alt.binding_select(
        options=routes_list,
        name="Routes",
    )

    # Column that controls the bar charts
    route_selector = alt.selection_point(
        fields=["route_combined_name"],
        bind=route_dropdown,
    )

    # Data
    # Filter for only schedule and vp
    df_sched_vp_both = df[df.sched_rt_category == "schedule_and_vp"].reset_index(
        drop=True
    )

    # Filter for only rows categorized as found in schedule and vp and all_day
    all_day = df_sched_vp_both.loc[
        df_sched_vp_both.time_period == "all_day"
    ].reset_index(drop=True)

    # Create route stats table for the text tables
    route_stats_df = route_stats(df)

    # Manipulate the df for some of the metrics
    timeliness_df = timeliness_trips(df_sched_vp_both)
    rt_journey_vp = pct_vp_journey(
        all_day, "pct_rt_journey_atleast1_vp", "pct_rt_journey_atleast2_vp"
    )
    sched_journey_vp = pct_vp_journey(
        all_day, "pct_rt_journey_atleast1_vp", "pct_rt_journey_atleast2_vp"
    )

    # Charts
    avg_scheduled_min = (
        grouped_bar_chart(
            df=all_day.drop_duplicates(),
            color_col="direction_id",
            y_col="avg_scheduled_service_minutes",
            offset_col="direction_id",
            title="Average Scheduled Minutes",
            subtitle="The average minutes a trip is scheduled to run.",
        )
        .add_params(route_selector)
        .transform_filter(route_selector)
    )

    timeliness_trips_dir_0 = (
        (
            base_facet_chart(
                timeliness_df.loc[timeliness_df.direction_id == 0].drop_duplicates(),
                "value",
                "variable",
                "time_period",
                "Breakdown of Trips by Categories for Direction 0",
                "Categorizing whether a trip is early, late, or ontime. A trip is on time if it arrives 5 minutes later or earlier than scheduled.",
            )
        )
        .add_params(route_selector)
        .transform_filter(route_selector)
    )
    timeliness_trips_dir_1 = (
        (
            base_facet_chart(
                timeliness_df.loc[timeliness_df.direction_id == 1].drop_duplicates(),
                "value",
                "variable",
                "time_period",
                "Breakdown of Trips by Categories for Direction 1",
                "Categorizing whether a trip is early, late, or ontime. A trip is on time if it arrives 5 minutes later or earlier than scheduled.",
            )
        )
        .add_params(route_selector)
        .transform_filter(route_selector)
    )

    frequency = (
        frequency_chart(df_sched_vp_both)
        .add_params(route_selector)
        .transform_filter(route_selector)
    )
    speed = (
        base_facet_line(
            df_sched_vp_both,
            "speed_mph",
            "Average Speed",
            "The average miles per hour the bus travels by direction and time of day.",
        )
        .add_params(route_selector)
        .transform_filter(route_selector)
    )

    vp_per_min = (
        (
            base_facet_with_ruler_chart(
                all_day.drop_duplicates(),
                "vp_per_minute",
                "ruler_for_vp_per_min",
                "Vehicle Positions per Minute",
                "Trips should have 2+ vehicle positions per minute.",
            )
        )
        .add_params(route_selector)
        .transform_filter(route_selector)
    )

    rt_vp_per_min = (
        base_facet_circle(
            rt_journey_vp,
            "value",
            "ruler_100_pct",
            "Percentage of Realtime Trips with 1+ and 2+ Vehicle Positions",
            "The goal is for almost 100% of trips to have 2 or more Vehicle Positions per minute.",
        )
        .add_params(route_selector)
        .transform_filter(route_selector)
    )
    sched_vp_per_min = (
        base_facet_circle(
            sched_journey_vp,
            "value",
            "sched_journey_vp",
            "Percentage of Scheduled Trips with 1+ and 2+ Vehicle Positions",
            "The goal is for almost 100% of trips to have 2 or more Vehicle Positions per minute.",
        )
        .add_params(route_selector)
        .transform_filter(route_selector)
    )
    spatial_accuracy = (
        base_facet_with_ruler_chart(
            all_day.drop_duplicates(),
            "pct_in_shape",
            "ruler_100_pct",
            "Spatial Accuracy",
            "The percentage of vehicle positions that fall within the static scheduled route shape reflects the accuracy of the spatial, realtime data.",
        )
        .add_params(route_selector)
        .transform_filter(route_selector)
    )

    text_dir0 = (
        (create_text_table(route_stats_df, 0))
        .add_params(route_selector)
        .transform_filter(route_selector)
    )
    text_dir1 = (
        create_text_table(route_stats_df, 1)
        .add_params(route_selector)
        .transform_filter(route_selector)
    )
    chart_list = [
        avg_scheduled_min,
        timeliness_trips_dir_0,
        timeliness_trips_dir_1,
        frequency,
        speed,
        vp_per_min,
        rt_vp_per_min,
        sched_vp_per_min,
        spatial_accuracy,
        text_dir0,
        text_dir1,
    ]

    chart = alt.vconcat(*chart_list).properties(
        resolve=alt.Resolve(
            scale=alt.LegendResolveMap(color=alt.ResolveMode("independent"))
        )
    )
    return chart