"""
Common functions for standardizing how outputs
are displayed in portfolio.
"""
import base64
from pathlib import Path

import pandas as pd
import yaml
from shared_utils import rt_utils


def decode_base64_url(row):
    """
    Provide decoded version of URL as ASCII.
    WeHo gets an incorrect padding, but urlsafe_b64decode works.
    Just in case, return uri truncated.
    """
    try:
        decoded = base64.urlsafe_b64decode(row.base64_url).decode("ascii")
    except base64.binascii.Error:
        decoded = row.uri.split("?")[0]

    return decoded


# https://github.com/cal-itp/data-analyses/blob/main/rt_delay/utils.py
def add_route_name(df: pd.DataFrame) -> pd.DataFrame:
    """
    Input a df that has route_id and route_short_name, route_long_name, route_desc, and this will pick
    """
    route_cols = ["route_id", "route_short_name", "route_long_name", "route_desc"]

    if not (set(route_cols).issubset(set(list(df.columns)))):
        raise ValueError(f"Input a df that contains {route_cols}")

    df = df.assign(route_name_used=df.apply(lambda x: rt_utils.which_desc(x), axis=1))

    # If route names show up with leading comma
    df = df.assign(route_name_used=df.route_name_used.str.lstrip(",").str.strip())

    return df


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


def create_portfolio_yaml_chapters_with_sections(
    portfolio_site_yaml: Path,
    df: pd.DataFrame,
    chapter_info: dict = {
        "column": "caltrans_district",
        "name": "district",
        "caption_prefix": "District ",
        "caption_suffix": "",
    },
    section_info: dict = {
        "column": "organization_name",
        "name": "organization_name",
    },
):
    """
    Overwrite a portfolio site yaml by filling in all the parameters.
    Chapters with sections refer to nested analyses.
    An example is a report parameterized for the transit operator,
    and several operators are grouped by under a Caltrans District.
    The operator pages are accessed by a dropdown under Caltrans District.

    portfolio_site_yaml: str | Path
        relative path to where the yaml is for portfolio
        '../portfolio/sites/gtfs_digest.yml'

    Example: Use the column "caltrans_district" which holds values like
    "04 - Oakland". We want to display "District 04 - Oakland, CA",
    so we can make use of prefix and suffix.

    chapter_info: dict = {
        "column": "caltrans_district",
        # column from df for parameterized values
        "name": "district",
        # name is the label/key on the yaml
        "caption_prefix": "District ",
        "caption_suffix": ", CA"
        # caption format is caption_prefix + chapter_value + caption_suffix

    },
    section_info: dict = {
        "column": "organization_name",
        "name": "organization",
    }
    """
    chapter_col = chapter_info["column"]
    chapter_values = sorted(list(df[chapter_col].unique()))

    # Eric's example
    # https://github.com/cal-itp/data-analyses/blob/main/rt_delay/04_generate_all.ipynb
    with open(portfolio_site_yaml) as f:
        site_yaml_dict = yaml.load(f, yaml.Loader)

    # Loop through each chapter (district), grab the sections (operators)
    section_col = section_info["column"]
    caption_prefix = chapter_info["caption_prefix"]
    caption_suffix = chapter_info["caption_suffix"]

    chapters_list = [
        {
            **{
                "caption": f"{caption_prefix}{one_chapter_value}{caption_suffix}",
                "params": {chapter_info["name"]: str(one_chapter_value)},
                "sections": [
                    {section_info["name"]: str(one_section_value)}
                    for one_section_value in df[df[chapter_col] == one_chapter_value][section_col].unique().tolist()
                ],
            }
        }
        for one_chapter_value in chapter_values
    ]

    # Make this into a list item
    parts_list = [{"chapters": chapters_list}]
    site_yaml_dict["parts"] = parts_list

    # dump this dict into the yaml and overwrite existing file
    output = yaml.dump(site_yaml_dict)

    with open(portfolio_site_yaml, "w") as f:
        f.write(output)

    print(f"{portfolio_site_yaml} generated")

    return


CALTRANS_DISTRICT_DICT = {
    # old name variations (key): portfolio name displayed (value)
    "03 - Marysville": "03 - Marysville / Sacramento",
    "04 - Oakland": "04 - Bay Area / Oakland",
    "05 - San Luis Obispo": "05 - San Luis Obispo / Santa Barbara",
    "06 - Fresno": "06 - Fresno / Bakersfield",
    "07 - Los Angeles": "07 - Los Angeles / Ventura",
    "08 - San Bernardino": "08 - San Bernardino / Riverside",
    "12 - Irvine": "12 - Santa Ana",
    "12 - Orange County": "12 - Santa Ana",
    **{
        k: k
        for k in [
            "01 - Eureka",
            "02 - Redding",
            "03 - Marysville / Sacramento",
            "04 - Bay Area / Oakland",
            "05 - San Luis Obispo / Santa Barbara",
            "06 - Fresno / Bakersfield",
            "07 - Los Angeles / Ventura",
            "08 - San Bernardino / Riverside",
            "09 - Bishop",
            "10 - Stockton",
            "11 - San Diego",
            "12 - Santa Ana",
        ]
    },
}


def standardize_portfolio_organization_names(df: pd.DataFrame, preferred_organization_name_dict: dict) -> pd.DataFrame:
    """
    Map the preferred organization name using schedule_gtfs_dataset_name.
    """
    df = df.assign(portfolio_organization_name=df.name.map(preferred_organization_name_dict))
    # drop the ones that were removed with duplicated feed info (create_portfolio_display_yaml.py)
    df = df.dropna(subset="portfolio_organization_name")

    return df
