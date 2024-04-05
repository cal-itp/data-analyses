import altair as alt
import calitp_data_analysis.magics
import geopandas as gpd
import great_tables as gt
import pandas as pd
from calitp_data_analysis import calitp_color_palette as cp
from great_tables import md
from IPython.display import HTML, Markdown, display
from segment_speed_utils.project_vars import RT_SCHED_GCS, SCHED_GCS
from shared_utils import rt_dates, rt_utils

"""
General Functions
"""
def labeling(word: str) -> str:
    return (
        word.replace("_", " ")
        .title()
        .replace("N", "Total")
        .replace("Pct", "%")
        .replace("Vp", "VP")
    )

def reverse_snakecase(df):
    """
    Clean up columns to remove underscores and spaces.
    """
    df.columns = df.columns.str.replace("_", " ").str.strip().str.title()
    return df

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

def two_vps_tag(row):
    if row.pct_rt_journey_atleast2_vp < 0.25:
        return "<25% of trip has 2+ pings"
    elif 0.25 <= row.pct_rt_journey_atleast2_vp < 0.51:
        return "25-50% of trip has 2+ pings"
    elif 0.51 <= row.pct_rt_journey_atleast2_vp < 0.76:
        return "51-75% of trip has 2+ pings"
    elif 0.76 <= row.pct_rt_journey_atleast2_vp < 0.99:
        return "76-99% of trip has 2+ pings"
    elif row.pct_rt_journey_atleast2_vp == 1:
        return "100% of trip has 2+ pings"
    else:
        return "No Info"
    
def spatial_accuracy_tag(row):
    if row.pct_in_shape < 0.5:
        return "<50% of VPs in shape"
    elif 0.5 <= row.pct_in_shape < 0.76:
        return "50-75% of VPs in shape"
    elif 0.76 <= row.pct_in_shape < 1:
        return "76-99% of VPs in shape"
    elif row.pct_in_shape == 1:
        return "100% of VPs in shape"
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
    df["pct_rt_journey_atleast2_vp_cat"] = df.apply(two_vps_tag, axis=1)
    df["pct_in_shape_cat"] = df.apply(spatial_accuracy_tag, axis=1)
    df["vp_per_minute_cat"] = df.apply(vp_per_min_tag, axis=1)    
    
    return df

def load_schedule_vp_metrics(name:str)->pd.DataFrame:
    df = pd.read_parquet(
    f"{RT_SCHED_GCS}digest/schedule_vp_metrics.parquet",
    filters=[[("name", "==", name)]])
    
    # Categorize
    df = add_categories(df)
    
    # Round float columns
    float_columns = df.select_dtypes(include=['float'])
    for i in float_columns:
        df[i] = df[i].round(2)
    
    # Neaten up some columns
    """
    df.sched_rt_category = (
    df.sched_rt_category.str.replace("_", " ")
    .str.title()
    .str.replace("Vp", "VP"))
    """
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
        )
        .merge(offpeak_stats, on=route_merge_cols)
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

"""
Scheduled_service_by_route
Functions
"""
def tag_day(df: pd.DataFrame) -> pd.DataFrame:
    # Function to determine if a date is a weekend day or a weekday
    def which_day(date):
        if date == 1:
            return "Monday"
        elif date == 2:
            return "Tuesday"
        elif date == 3:
            return "Wednesday"
        elif date == 4:
            return "Thursday"
        elif date == 5:
            return "Friday"
        elif date == 6:
            return "Saturday"
        else:
            return "Sunday"

    # Apply the function to each value in the "service_date" column
    df["day_type"] = df["day_type"].apply(which_day)

    return df

def load_scheduled_service(year:str, organization_name:str)->pd.DataFrame:
    df = pd.read_parquet(
    f"{SCHED_GCS}scheduled_service_by_route_{year}.parquet",
    filters=[[("name", "==", organization_name)]],)
    
    df["month"] = df["month"].astype(str).str.zfill(2)
    df["full_date"] = (df.month.astype(str) + "-" + df.year.astype(str))
    df = tag_day(df)
    
    return df

def summarize_monthly(year:str, organization_name:str)->pd.DataFrame:
    df = load_scheduled_service(year, organization_name)
    df2 = (
    df.groupby(
        [
            "full_date",
            "month",
            "name",
            "day_type",
            "time_of_day",
        ]
    )
    .agg(
        {
            "ttl_service_hours": "mean",
        }
    )
    .reset_index()
    )
    return df2

"""
Charts
"""
blue_palette = ["#B9D6DF", "#2EA8CE", "#0B405B"]

def single_bar_chart_dropdown(
    df: pd.DataFrame,
    x_col: str,
    y_col: str,
    offset_col: str,
    title: str,
    dropdown_col: str,
):
    dropdown_list = sorted(df[dropdown_col].unique().tolist())

    dropdown = alt.binding_select(options=dropdown_list, name=labeling(dropdown_col))

    selector = alt.selection_single(
        name=labeling(dropdown_col), fields=[dropdown_col], bind=dropdown
    )

    ruler = (
        alt.Chart(df)
        .mark_rule(color="red", strokeDash=[10, 7])
        .encode(y=f"mean({y_col}):Q")
    )

    chart = (
        alt.Chart(df)
        .mark_bar()
        .encode(
            x=alt.X(
                f"{x_col}:N",
                title="Day",
                scale=alt.Scale(
                    domain=[
                        "Monday",
                        "Tuesday",
                        "Wednesday",
                        "Thursday",
                        "Friday",
                        "Saturday",
                        "Sunday",
                    ]
                ),
            ),
            y=alt.Y(f"{y_col}:Q", title=labeling(y_col)),
            xOffset=f"{offset_col}:N",
            color=alt.Color(
                f"{offset_col}:N",
                scale=alt.Scale(
                    range=cp.CALITP_SEQUENTIAL_COLORS,
                ),
            ),
            tooltip=df.columns.tolist(),
        )
    )
    chart = (chart + ruler).properties(title=title, width=600, height=400)
    chart = chart.add_params(selector).transform_filter(selector)

    display(chart)
    
def grouped_bar_chart(
    df: pd.DataFrame,
    color_col: str,
    y_col: str,
    offset_col: str,
    title: str,
    subtitle: str,
):
    df = df.assign(
        time_period=df.time_period.str.replace("_", " ").str.title()
    ).reset_index(drop=True)

    df[y_col] = df[y_col].fillna(0).astype(int)
    tooltip_cols = [
        "direction_id",
        "time_period",
        "route_combined_name",
        "organization_name",
        "service_date",
        y_col,
    ]
    """ 
    ruler = (
        alt.Chart(df)
        .mark_rule(color="red", strokeDash=[10, 7])
        .encode(y=f"mean({y_col}):Q")
    )
    """
    chart = (
        alt.Chart(df)
        .mark_bar(size=10)
        .encode(
            x=alt.X(
                "yearmonthdate(service_date):O",
                title=["Grouped by Direction ID", "Date"],
                axis=alt.Axis(format="%b %Y"),
            ),
            y=alt.Y(f"{y_col}:Q", title=labeling(y_col)),
            xOffset=alt.X(f"{offset_col}:N", title=labeling(offset_col)),
            color=alt.Color(
                f"{color_col}:N",
                title=labeling(color_col),
                scale=alt.Scale(
                    range=blue_palette,
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

def heatmap(
    df: pd.DataFrame,
    color_col: str,
    title: str,
    subtitle1: str,
    subtitle2: str,
    subtitle3: str,
):
    df = df.assign(
        time_period=df.time_period.str.replace("_", " ").str.title()
    ).reset_index(drop=True)

    # Grab original column that wasn't categorized
    original_col = color_col.replace("_cat", "")

    tooltip_cols = [
        "direction_id",
        "time_period",
        "route_combined_name",
        "organization_name",
        color_col,
        original_col,
    ]

    chart = (
        alt.Chart(df)
        .mark_rect(size=30)
        .encode(
            x=alt.X(
                "yearmonthdate(service_date):O",
                axis=alt.Axis(labelAngle=-45, format="%b %Y"),
                title=["Grouped by Direction ID", "Service Date"],
            ),
            y=alt.Y("time_period:O", title=["Time Period"]),
            xOffset=alt.X(f"direction_id:N", title="Direction ID"),
            color=alt.Color(
                f"{color_col}:N",
                title=labeling(color_col),
                scale=alt.Scale(range=cp.CALITP_SEQUENTIAL_COLORS),
            ),
            tooltip=tooltip_cols,
        )
        .properties(
            title={"text": [title], "subtitle": [subtitle1, subtitle2, subtitle3]},
            width=500,
            height=300,
        )
    )

    text = chart.mark_text(baseline="middle").encode(
        alt.Text("direction_id"), color=alt.value("white")
    )

    final_chart = chart + text
    return final_chart

def base_facet_line(
    df: pd.DataFrame, y_col: str, title: str, subtitle: str
) -> alt.Chart:

    df = df.assign(
        time_period=df.time_period.str.replace("_", " ").str.title()
    ).reset_index(drop=True)
    # https://stackoverflow.com/questions/26454649/python-round-up-to-the-nearest-ten

    if "pct" in y_col:
        max_y = 1.2
    elif "per_minute" in y_col:
        max_y = round(df[y_col].max())
    else:
        max_y = round(df[y_col].max(), -1) + 5

    df[f"{y_col}_str"] = df[y_col].astype(str)

    ruler = (
        alt.Chart(df)
        .mark_rule(color="red", strokeDash=[10, 7])
        .encode(y=f"mean(speed_mph):Q")
    )

    chart = (
        alt.Chart(df)
        .mark_line(size=5)
        .encode(
            x=alt.X(
                "yearmonthdate(service_date):O",
                title="Date",
                axis=alt.Axis(format="%b %Y"),
            ),
            y=alt.Y(
                f"{y_col}:Q", title=labeling(y_col), scale=alt.Scale(domain=[0, max_y])
            ),
            color=alt.Color(
                "time_period:N",
                title=labeling("time_period"),
                scale=alt.Scale(range=blue_palette),
            ),
            tooltip=[
                "route_combined_name",
                "route_id",
                "direction_id",
                "time_period",
                f"{y_col}_str",
            ],
        )
    )

    chart = (chart + ruler).properties(width=220, height=300)
    chart = chart.facet(
        column=alt.Column("direction_id:N", title=labeling("direction_id")),
    ).properties(
        title={
        "text": [title], 
            "subtitle": [subtitle],
        }
    )
    return chart

def base_facet_bar(df: pd.DataFrame, direction_id: str):

    df = df.loc[df.direction_id == direction_id]
    ruler = (
        alt.Chart(df)
        .mark_rule(color="red", strokeDash=[10, 7])
        .encode(y=f"mean(speed_mph):Q")
    )

    chart = (
        alt.Chart(df.loc[df.direction_id == direction_id])
        .mark_bar(size=15, clip=True)
        .encode(
            x=alt.X(
                "yearmonthdate(service_date):O",
                title=["Service Date"],
                axis=alt.Axis(labelAngle=-45, format="%b %Y"),
            ),
            y=alt.Y(
                "value:Q",
                title=labeling("value"),
                scale=alt.Scale(
                    domain=[
                        (df.value.min()),
                        (df.value.max() + 10),
                    ]
                ),
            ),
            color=alt.Color(
                "variable:N",
                title=labeling("variable"),
                scale=alt.Scale(range=blue_palette),
            ),
            tooltip=df.columns.tolist(),
        )
    )

    chart = chart.properties(width=220, height=300).facet(
        column=alt.Column(
            "time_period:N",
        )
    ).properties(title = {"text": labeling(f"Timeliness of Trips for Direction {direction_id}"),
                "subtitle": [
                    "Categorizing whether trips are early ,late or on time.",
                    "A trip is considered on time even if it arrives five minutes later and earlier than scheduled."],}
    )
    

    return chart

def create_text_table(df: pd.DataFrame, direction_id: str):

    df = df.loc[df.direction_id == direction_id].reset_index(drop=True)
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
        title=f"Route Statistics for Direction {direction_id}", width=500, height=300
    )
    return text_chart

"""
Route-Direction
Reports
"""
def filtered_route(
    df: pd.DataFrame,
) -> alt.Chart:
    """
    https://stackoverflow.com/questions/58919888/multiple-selections-in-altair
    """

    route_dropdown = alt.binding_select(
        options=sorted(df["route_combined_name"].unique().tolist()),
        name="Routes ",
    )

    # Column that controls the bar charts
    route_selector = alt.selection_point(
        fields=["route_combined_name"],
        bind=route_dropdown,
    )

    # Data
    sched_df = df[df.sched_rt_category != "vp_only"]
    vp_df = df[df.sched_rt_category != "schedule_only"]
    route_stats_df = route_stats(df)
    timeliness_df = timeliness_trips(vp_df)

    # Charts

    speed = (
        base_facet_line(
            vp_df,
            "speed_mph",
            "Average Speed",
            "The average miles per hour a trip goes by direction and time of day.",
        )
        .add_params(route_selector)
        .transform_filter(route_selector)
    )

    frequency = (
        heatmap(
            sched_df,
            "frequency_cat",
            "Frequency of Route",
            "Frequency tracks the number of times per hour a route goes in one direction.",
            "Ex.: A frequency of 2.3 going in the direction 1 means the bus passes by this direction about",
            "twice an hour.",
        )
        .add_params(route_selector)
        .transform_filter(route_selector)
    )
    timeliness = (
        heatmap(
            sched_df,
            "rt_sched_journey_ratio_cat",
            "Realtime vs. Scheduled Trip Times",
            "Dividing actual trip time by scheduled trip time gives an estimate of how on-time a trip typically is.",
            "A ratio of 1.5 for a trip scheduled to last 1 hour means the trip lasted 1 hour and 30 minutes, thus.",
            " taking 50% longer than scheduled.",
        )
        .add_params(route_selector)
        .transform_filter(route_selector)
    )

    timeliness_trips_dir_0 = (
        (base_facet_bar(timeliness_df, 0))
        .add_params(route_selector)
        .transform_filter(route_selector)
    )

    timeliness_trips_dir_1 = (
        (base_facet_bar(timeliness_df, 1))
        .add_params(route_selector)
        .transform_filter(route_selector)
    )

    """
    This chart is a bit boring
    total_scheduled_trips = (
        grouped_bar_chart(
            df=sched_df.loc[sched_df.time_period != "all_day"],
            color_col="time_period",
            y_col="n_scheduled_trips",
            offset_col="direction_id",
            title="Total Scheduled Trips",
            subtitle="How many times per day this route is scheduled to run in one particular direction.",
        )
        .add_params(route_selector)
        .transform_filter(route_selector)
    )
    """
    atleast_2_vp = (
        heatmap(
            vp_df,
            "pct_rt_journey_atleast2_vp_cat",
            "% of trip with 2+ Vehicle Positions",
            "Dividing the number of minutes with 2+ vehicle positions by the total trip duration",
            "reflects the temporary coverage for a trip and in turn, signals the reliablity of real-time data. Ideally, vehicle positions should be recorded every 20 seconds.",
            "Ex.: A percentage of 0.3 indicates only 30% of the trip recorded 2+ vehicle positions per minute.",
        )
        .add_params(route_selector)
        .transform_filter(route_selector)
    )

    spatial_accuracy = (
        heatmap(
            vp_df,
            "pct_in_shape_cat",
            "Spatial Accuracy of Realtime Data",
            "By comparing vehicle positions produced by GTFS and the scheduled route shape,",
            " we can determine the percentage of actual positions that fell within a reasonable radius of the route.",
            "This percentage reflects the accuracy of the spatial data collected in real time.",
        )
        .add_params(route_selector)
        .transform_filter(route_selector)
    )

    route_stats_dir0 = (
        create_text_table(route_stats_df, 0)
        .add_params(route_selector)
        .transform_filter(route_selector)
    )

    route_stats_dir1 = (
        create_text_table(route_stats_df, 1)
        .add_params(route_selector)
        .transform_filter(route_selector)
    )

    chart_list = [
        speed,
        frequency,
        timeliness,
        timeliness_trips_dir_0,
        timeliness_trips_dir_1,
        atleast_2_vp,
        spatial_accuracy,
        route_stats_dir0,
        route_stats_dir1,
    ]

    #
    chart = alt.vconcat(*chart_list).properties(
        resolve=alt.Resolve(
            scale=alt.LegendResolveMap(color=alt.ResolveMode("independent"))
        )
    )
    return chart

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
    .tab_options(container_width="100%")
    .tab_options(table_font_size="12px"))
    
def route_categories(df):
    route_categories = (
    df[df.time_period == "all_day"]
    .groupby("sched_rt_category")
    .agg({"route_combined_name": "nunique"})
    .reset_index()
    )
    
    route_categories = route_categories.dropna()
    
    display(gt.GT(data=route_categories)
    .fmt_integer(columns=["route_combined_name"], compact=True)
    .cols_label(route_combined_name="Total Routes", sched_rt_category="Category")
    .tab_options(container_width="100%")
    .tab_header(
        title="Routes with GTFS Availability",
        subtitle="Schedule only indicates the route(s) were found only in static, schedule data. Vehicle Positions (VP) only indicates the route(s) were found only in real-time data.",
    )
    .tab_options(table_font_size="12px")
)