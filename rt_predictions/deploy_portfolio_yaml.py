"""
Create the yamls for parameterized reports.

Since these yamls do not use sections, we can
generate them similarly using Makefile commands.

Try out typer to make CLI a little easier to use, since
this only takes 1 argument with 2 possible values.
Base it off of this tutorial:
https://typer.tiangolo.com/tutorial/options/required/
"""

from pathlib import Path

import gcsfs
import google.auth
import pandas as pd
import report_utils
import typer
from shared_utils import portfolio_utils

RT_TRIP_UPDATES_STOP_YAML = Path("../portfolio/sites/rt_stop_metrics.yml")
RT_TRIP_UPDATES_OPERATOR_YAML = Path("../portfolio/sites/rt_operator_metrics.yml")

credentials, project = google.auth.default()
fs = gcsfs.GCSFileSystem()

app = typer.Typer()


def check_stop_and_route_counts(one_month: str):
    """
    Filter the dfs the same way in the stop report,
    and grab the operators that can be populated .
    """
    filtering = [
        [
            ("month_first_day", "==", pd.to_datetime(one_month)),
            ("schedule_name", "!=", "Bay Area 511 Regional Schedule"),
            ("day_type", "==", "Weekday"),  # for operator report, show day_types
        ]
    ]

    stop_df = (
        report_utils.import_stop_df(filters=filtering, columns=["schedule_name", "route_name", "geometry"])
        .dropna(subset=["route_name", "geometry"])
        .drop(columns="geometry")
        .drop_duplicates()
    )

    route_df = (
        report_utils.import_route_df(
            filters=filtering,
            columns=[
                "schedule_name",
                "tu_name",
                "route_name",
            ],
        )
        .dropna(subset="route_name")
        .drop_duplicates()
    )

    # Use inner merge - charts and maps need both dfs to work
    count_df = (
        pd.merge(route_df, stop_df, on=["schedule_name", "route_name"], how="inner")
        .groupby(["schedule_name", "tu_name"])
        .agg({"route_name": "count"})
        .reset_index()
    )

    return count_df


def check_route_counts(one_month: str):
    """
    Filter the dfs the same way in the route report,
    and grab the operators that can be populated .
    """
    filtering = [
        [
            ("month_first_day", "==", pd.to_datetime(one_month)),
            ("schedule_name", "!=", "Bay Area 511 Regional Schedule"),
        ]
    ]

    # TODO: should portfolio_utils be extended to include multiple params that move together?
    # simpler to have 1 param. Here, tu_name would be used, since the same schedule_name
    # can appear for different tu_names (Marin Swiftly/Equans or Torrance).
    route_df = (
        report_utils.import_route_df(
            filters=filtering,
            columns=[
                "schedule_name",
                "tu_name",
            ],
        )
        .drop_duplicates()
        .reset_index(drop=True)
    )

    return route_df


@app.command()
def overwrite_yaml(name: str = typer.Argument(default="rt_msa"), month: str = ""):
    """
    Create yamls for portfolio.
    """
    if name == "rt_msa_stops":
        print(month)
        df = check_stop_and_route_counts(month)

        portfolio_utils.create_portfolio_yaml_chapters_no_sections(
            RT_TRIP_UPDATES_STOP_YAML, chapter_name="name", chapter_values=sorted(list(df.tu_name))
        )

    elif name == "rt_msa_operators":
        print(month)
        df = check_route_counts(month)

        portfolio_utils.create_portfolio_yaml_chapters_no_sections(
            RT_TRIP_UPDATES_OPERATOR_YAML, chapter_name="name", chapter_values=sorted(list(df.tu_name))
        )

    return


if __name__ == "__main__":
    app()
