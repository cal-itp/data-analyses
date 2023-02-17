"""
Common functions for standardizing how outputs are displayed in portfolio.

1. Standardize operator level naming for reports.
With GTFS data, analysis is done with identifiers (calitp_itp_id, route_id, etc).
When publishing reports, we should be referring to operator name,
route name, Caltrans district the same way.

2. Table styling for pandas styler objects
need to import different pandas to add type hint for styler object

"""
import base64

import dask.dataframe as dd
import dask_geopandas as dg
import pandas as pd
import pandas.io.formats.style  # for type hint: https://github.com/pandas-dev/pandas/issues/24884
from calitp.tables import tbls
from IPython.display import HTML
from shared_utils import gtfs_utils_v2, rt_utils
from siuba import *

district_name_dict = {
    1: "District 1 - Eureka",
    2: "District 2 - Redding",
    3: "District 3 - Marysville",
    4: "District 4 - Oakland",
    5: "District 5 - San Luis Obispo",
    6: "District 6 - Fresno",
    7: "District 7 - Los Angeles",
    8: "District 8 - San Bernardino",
    9: "District 9 - Bishop",
    10: "District 10 - Stockton",
    11: "District 11 - San Diego",
    12: "District 12 - Irvine",
}


def clean_organization_name(df: pd.DataFrame) -> pd.DataFrame:
    """
    Clean up organization name used in portfolio.
    """
    df = df.assign(
        name=(
            df.name.str.replace("Schedule", "")
            .str.replace("Vehicle Positions", "")
            .str.replace("VehiclePositions", "")
            .str.replace("Trip Updates", "")
            .str.replace("TripUpdates", "")
            .str.replace("Service Alerts", "")
            .str.replace("Bay Area 511", "")
            .str.strip()
        )
    )

    return df


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


def add_agency_identifiers(df: pd.DataFrame) -> pd.DataFrame:
    """
    Find the current base64_url for the organization name and
    decode it as ASCII (Chad Baker request for CKAN data).
    The encoded version might not be as usable for users.
    """
    dim_gtfs_datasets = gtfs_utils_v2.get_transit_organizations_gtfs_dataset_keys(
        keep_cols=None, get_df=True
    )

    current_feeds = (
        dim_gtfs_datasets[
            (dim_gtfs_datasets.data_quality_pipeline == True)
            & (dim_gtfs_datasets._is_current == True)
        ]
        .drop_duplicates(subset="name")
        .reset_index(drop=True)
    )

    current_feeds2 = current_feeds.assign(
        feed_url=current_feeds.apply(lambda x: decode_base64_url(x), axis=1)
    )

    df2 = pd.merge(
        df,
        current_feeds2[["name", "base64_url", "feed_url", "uri"]],
        on="name",
        how="inner",
        validate="m:1",
    )

    return df2


def standardize_gtfs_dataset_names(
    df: pd.DataFrame, name_col: str = "name"
) -> pd.DataFrame:
    """
    Have gtfs_dataset_name reflect the operator.
    Remove the distinction between LA Metro Bus and LA Metro Rail (which
    show up as 2 feeds, 2 feed_keys, 2 gtfs_dataset_keys in our warehouse).
    """
    df[name_col] = (
        df[name_col]
        .str.replace("LA Metro Bus", "LA Metro")
        .str.replace("LA Metro Rail", "LA Metro")
    )

    return df


# https://github.com/cal-itp/data-analyses/blob/main/bus_service_increase/E5_make_stripplot_data.py
def add_caltrans_district() -> pd.DataFrame:
    """
    Returns a dataframe with calitp_itp_id and the caltrans district
    """
    df = (
        (
            tbls.airtable.california_transit_organizations()
            >> select(_.itp_id, _.caltrans_district)
            >> distinct()
            >> collect()
            >> filter(_.itp_id.notna())
        )
        .sort_values(["itp_id", "caltrans_district"])
        .drop_duplicates(subset="itp_id")
        .rename(columns={"itp_id": "calitp_itp_id"})
        .astype({"calitp_itp_id": int})
        .reset_index(drop=True)
    )

    # Some ITP IDs are missing district info
    missing_itp_id_district = {
        48: "03 - Marysville",  # B-Line in Butte County
        70: "04 - Oakland",  # Cloverdale Transit in Sonoma County
        82: "02 - Redding",  # Burney Express in Redding
        142: "12 - Irvine",  # Irvine Shuttle
        171: "07 - Los Angeles",  # Avocado Heights/Bassett/West Valinda Shuttle
        473: "05 - San Luis Obispo",  # Clean Air Express in Santa Barbara
    }

    missing_ones = pd.DataFrame(
        missing_itp_id_district.items(), columns=["calitp_itp_id", "caltrans_district"]
    )

    # If there are any missing district info for ITP IDs, fill it in now
    df2 = (
        pd.concat([df, missing_ones], axis=0)
        .sort_values("calitp_itp_id")
        .reset_index(drop=True)
    )

    return df2


# https://github.com/cal-itp/data-analyses/blob/main/rt_delay/utils.py
def add_route_name(df: pd.DataFrame) -> pd.DataFrame:
    """
    Input a df that has route_id and route_short_name, route_long_name, route_desc, and this will pick
    """
    route_cols = ["route_id", "route_short_name", "route_long_name", "route_desc"]

    if not (set(route_cols).issubset(set(list(df.columns)))):
        raise ValueError(f"Input a df that contains {route_cols}")

    if isinstance(df, pd.DataFrame):
        ddf = dd.from_pandas(df, npartitions=2)
    elif isinstance(df, gpd.GeoDataFrame):
        ddf = dg.from_geopandas(df, npartitions=2)

    ddf = ddf.assign(
        route_name_used=ddf.apply(
            lambda x: rt_utils.which_desc(x), axis=1, meta=("route_name_used", "str")
        )
    )

    df = ddf.compute()

    # If route names show up with leading comma
    df = df.assign(route_name_used=df.route_name_used.str.lstrip(",").str.strip())

    return df


def add_custom_format(
    df_style: pd.io.formats.style.Styler,
    format_str: str,
    cols_to_format: list,
) -> pd.io.formats.style.Styler:
    """
    Appends any additional formatting needs.
        key: format string, such as '{:.1%}'
        value: list of columns to apply that formatter to.
    """
    new_styler = df_style.format(
        subset=cols_to_format, formatter={c: format_str for c in cols_to_format}
    )

    return new_styler


def style_table(
    df: pd.DataFrame,
    rename_cols: dict = {},
    drop_cols: list = [],
    integer_cols: list = [],
    one_decimal_cols: list = [],
    two_decimal_cols: list = [],
    three_decimal_cols: list = [],
    currency_cols: list = [],
    percent_cols: list = [],
    left_align_cols: list = "first",  # by default, left align first col
    center_align_cols: list = "all",  # by default, center align all other cols
    right_align_cols: list = [],
    custom_format_cols: dict = {},
    display_table: bool = True,
    display_scrollbar: bool = False,
    scrollbar_font: str = "12px",
    scrollbar_height: str = "400px",
    scrollbar_width: str = "fit-content",
) -> pd.io.formats.style.Styler:
    """
    Returns a pandas Styler object with some basic formatting.
    Even if display_table is True, pandas Styler object returned,
    just with a display() happening in the notebook cell.
    Any other tweaks for currency, percentages, etc should be done before / after,
    if it can't be put into custom_format_cols.

    custom_format_cols = {
        '{:,.1%}': ['colA', 'colB']
    }

    Generalize with dict comprehension or list comprehension
    list comprehension: df.style.format(subset=percent_cols,  **{'formatter': '{:,.2%}'})
    dict comprehension: df.style.format(formatter = {c: '{:,.2%}' for c in percent_cols})
    """
    df = (
        df.drop(columns=drop_cols)
        .rename(columns=rename_cols)
        .astype({c: "Int64" for c in integer_cols})
    )

    if left_align_cols == "first":
        left_align_cols = list(df.columns)[0]
    if center_align_cols == "all":
        center_align_cols = list(df.columns)
        # all other columns except first one is center aligned
        center_align_cols = [c for c in center_align_cols if c not in left_align_cols]

    # Use dictionary to do mapping of formatter to columns
    formatter_dict = {
        "{:,g}": integer_cols,
        "{:,.1f}": one_decimal_cols,
        "{:,.2f}": two_decimal_cols,
        "{:,.3f}": three_decimal_cols,
        "{:,.2%}": percent_cols,
        "$ {:,.2f}": currency_cols,
    }

    # Add in custom format dict
    entire_formatter_dict = {**formatter_dict, **custom_format_cols}

    df_style = (
        df.style.set_properties(subset=left_align_cols, **{"text-align": "left"})
        .set_properties(subset=center_align_cols, **{"text-align": "center"})
        .set_properties(subset=right_align_cols, **{"text-align": "right"})
        .set_table_styles([dict(selector="th", props=[("text-align", "center")])])
        # .hide(axis="index") # pandas >= 1.4
        .hide_index()  # pandas < 1.4
    )

    for format_str, cols_to_format in entire_formatter_dict.items():
        df_style = add_custom_format(df_style, format_str, cols_to_format)

    # https://stackoverflow.com/questions/63686157/flexible-chaining-in-python-pandas
    if display_table:
        if display_scrollbar:
            display(
                HTML(
                    f"<div style='height: {scrollbar_height}; overflow: auto; width: {scrollbar_width}'>"
                    + (
                        df_style.set_properties(
                            **{"font-size": scrollbar_font}
                        ).render()
                    )
                    + "</div>"
                )
            )
        else:
            display(HTML(df_style.to_html()))

    return df_style
