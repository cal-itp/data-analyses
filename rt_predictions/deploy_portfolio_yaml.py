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
import yaml

# from shared_utils import portfolio_utils

RT_TRIP_UPDATES_STOP_YAML = Path("../portfolio/sites/rt_trip_updates_stop_metrics.yml")

credentials, project = google.auth.default()
fs = gcsfs.GCSFileSystem()

app = typer.Typer()


def create_portfolio_yaml_chapters_no_sections(portfolio_site_yaml: Path, chapter_name: str, chapter_values: list):
    """
    Overwrite a portfolio site yaml by filling in all the parameters.
    Chapters no sections refer to analyses parameterized by 1 value.
    An example is a report parameterized for each Caltrans District,
    where each district has a page, but there is no dropdown below the district.

    chapter_name: this is the label/key on the yaml

    chapter_values: list of values used to parameterize notebook
        ex: list of districts [1, 2, 3, ..., 12]
        ex: list of district names ["04 - Oakland", "07 - Los Angeles"]
    """
    with open(portfolio_site_yaml) as f:
        site_yaml_dict = yaml.load(f, yaml.Loader)

    chapters_list = [{**{"params": {chapter_name: str(one_chapter_value)}}} for one_chapter_value in chapter_values]

    # Make this into a list item
    parts_list = [{"caption": "Introduction"}, {"chapters": chapters_list}]
    site_yaml_dict["parts"] = parts_list

    # dump this dict into the yaml and overwrite existing file
    output = yaml.dump(site_yaml_dict)

    with open(portfolio_site_yaml, "w") as f:
        f.write(output)

    print(f"{portfolio_site_yaml} generated")

    return


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


@app.command()
def overwrite_yaml(name: str = typer.Argument(default="rt_msa"), month: str = ""):
    """
    Create yamls for portfolio.
    """
    if name == "rt_msa_stops":
        print(month)
        df = check_stop_and_route_counts(month)

        # TODO: extend for ability to do 2 entries, tu_name/schedule_name
        create_portfolio_yaml_chapters_no_sections(
            RT_TRIP_UPDATES_STOP_YAML, chapter_name="name", chapter_values=sorted(list(df.tu_name.unique()))
        )

    return


if __name__ == "__main__":
    app()
