import pandas as pd
import _operator_grain_route_dir_visuals as _report_route_dir_visuals
from update_vars import GTFS_DATA_DICT, RT_SCHED_GCS
import altair as alt
from omegaconf import OmegaConf
readable_dict = OmegaConf.load("readable2.yml")

readable_col_names = {
    "analysis_name": "Analysis Name",
    "month_year": "Month",
    "weekday_weekend": "Weekday or Weekend",
    "departure_hour": "Departure Hour (in Military Time)",
    "service_hours": "Service Hours",
    "daily_service_hours": "Daily Service Hours",
}

def prep_operator_service_hours() -> pd.DataFrame:
    """
    Load dataframe with the total scheduled service hours
    a transit operator.
    """
    SCHEDULED_SERVICES_FILE = GTFS_DATA_DICT.digest_tables.scheduled_service_hours
    SCHEDULED_SERVICES_REPORT = GTFS_DATA_DICT.digest_tables.scheduled_service_hours_report
    
    url = f"{GTFS_DATA_DICT.digest_tables.dir}{SCHEDULED_SERVICES_FILE}.parquet"
    
    df = pd.read_parquet(url)

    # Rename dataframe
    df = df.rename(columns=readable_col_names)
    
    # Save out the dataframe
    df.to_parquet(f"{GTFS_DATA_DICT.digest_tables.dir}{SCHEDULED_SERVICES_REPORT}.parquet")
    # print(df.columns)
    return df

def create_bg_service_chart() -> alt.Chart:
    """
    Create a shaded background for the Service Hour Chart
    to differentiate between time periods.
    """
    specific_chart_dict = readable_dict.background_graph
    cutoff = pd.DataFrame(
        {
            "start": [0, 4, 7, 10, 15, 19],
            "stop": [3.99, 6.99, 9.99, 14.99, 18.99, 24],
            "Time Period": [
                "Owl:12-3:59AM",
                "Early AM:4-6:59AM",
                "AM Peak:7-9:59AM",
                "Midday:10AM-2:59PM",
                "PM Peak:3-7:59PM",
                "Evening:8-11:59PM",
            ],
        }
    )

    # Sort legend by time, 12am starting first.
    chart = (
        alt.Chart(cutoff.reset_index())
        .mark_rect(opacity=0.15)
        .encode(
            x="start",
            x2="stop",
            y=alt.value(0),
            y2=alt.value(250),
            color=alt.Color(
                "Time Period:N",
                sort=(
                    [
                        "Owl:12-3:59AM",
                        "Early AM:4-6:59AM",
                        "AM Peak:7-9:59AM",
                        "Midday:10AM-2:59PM",
                        "PM Peak:3-7:59PM",
                        "Evening:8-11:59PM",
                    ]
                ),
                scale=alt.Scale(
                    range=[*specific_chart_dict.colors]
                ),
            ),
        )
    )

    return chart

def scheduled_service_hr_graph(
    df: pd.DataFrame, weekday_weekend: str, specific_chart_dict: dict
) -> alt.Chart:
    df2 = df.loc[df["Weekday or Weekend"] == weekday_weekend]

    # Create an interactive legend so you can view one time period at a time.
    selection = alt.selection_point(fields=["Month"], bind="legend")

    line = _report_route_dir_visuals.line_chart(
        df=df2,
        x_col="Departure Hour (in Military Time)",
        y_col="Daily Service Hours",
        color_col="Month",
        color_scheme=[*specific_chart_dict.colors],
        tooltip_cols=[*specific_chart_dict.tooltip],
        date_format="",
    )

    bg = create_bg_service_chart()
    chart = (line + bg).properties(
        resolve=alt.Resolve(
            scale=alt.LegendResolveMap(color=alt.ResolveMode("independent"))
        )
    )

    chart = _report_route_dir_visuals.configure_chart(
        chart=chart,
        width=400,
        height=250,
        title=specific_chart_dict.title,
        subtitle=specific_chart_dict.subtitle,
    )

    chart = chart.add_params(selection)

    return chart

if __name__ == "__main__":
    
    df = prep_operator_service_hours()
    