"""
Common functions for standardizing how outputs are displayed in portfolio.

1. Standardize operator level naming for reports.
With GTFS data, analysis is done with identifiers (calitp_itp_id, route_id, etc).
When publishing reports, we should be referring to operator name,
route name, Caltrans district the same way.

2. Table styling for pandas styler objects
need to import different pandas to add type hint for styler object

"""
# os.environ["USE_PYGEOS"] = "0"  # avoids import warning but flake doesn't statements before imports...
import base64

# import os
from typing import Union

import dask.dataframe as dd
import dask_geopandas as dg
import geopandas as gpd
import pandas as pd
import pandas.io.formats.style  # for type hint: https://github.com/pandas-dev/pandas/issues/24884
from IPython.display import HTML
from shared_utils import rt_utils
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
def add_route_name(df: Union[pd.DataFrame, dd.DataFrame]) -> Union[pd.DataFrame, dd.DataFrame]:
    """
    Input a df that has route_id and route_short_name, route_long_name, route_desc, and this will pick
    """
    route_cols = ["route_id", "route_short_name", "route_long_name", "route_desc"]

    if not (set(route_cols).issubset(set(list(df.columns)))):
        raise ValueError(f"Input a df that contains {route_cols}")

    if isinstance(df, (dd.DataFrame, dg.GeoDataFrame)):
        df = df.assign(
            route_name_used=df.apply(lambda x: rt_utils.which_desc(x), axis=1, meta=("route_name_used", "str"))
        )
    else:
        df = df.assign(route_name_used=df.apply(lambda x: rt_utils.which_desc(x), axis=1))

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
    new_styler = df_style.format(subset=cols_to_format, formatter={c: format_str for c in cols_to_format})

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
    df = df.drop(columns=drop_cols).rename(columns=rename_cols).astype({c: "Int64" for c in integer_cols})

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
                    + (df_style.set_properties(**{"font-size": scrollbar_font}).render())
                    + "</div>"
                )
            )
        else:
            display(HTML(df_style.to_html()))

    return df_style


def aggregate_by_geography(
    df: Union[pd.DataFrame, gpd.GeoDataFrame],
    group_cols: list,
    sum_cols: list = [],
    mean_cols: list = [],
    count_cols: list = [],
    nunique_cols: list = [],
    rename_cols: bool = False,
) -> pd.DataFrame:
    """
    df: pandas.DataFrame or geopandas.GeoDataFrame.,
        The df on which the aggregating is done.
        If it's a geodataframe, it must exclude the tract's geometry column

    group_cols: list.
        List of columns to do the groupby, but exclude geometry.
    sum_cols: list.
        List of columns to calculate a sum with the groupby.
    mean_cols: list.
        List of columns to calculate an average with the groupby
        (beware: may want weighted averages and not simple average!!).
    count_cols: list.
        List of columns to calculate a count with the groupby.
    nunique_cols: list.
        List of columns to calculate the number of unique values with the groupby.
    rename_cols: boolean.
        Defaults to False. If True, will rename columns in sum_cols to have suffix `_sum`,
        rename columns in mean_cols to have suffix `_mean`, etc.

    Returns a pandas.DataFrame or geopandas.GeoDataFrame (same as input).
    """
    final_df = df[group_cols].drop_duplicates().reset_index()

    def aggregate_and_merge(
        df: Union[pd.DataFrame, gpd.GeoDataFrame],
        final_df: pd.DataFrame,
        group_cols: list,
        agg_cols: list,
        aggregate_function: str,
    ):
        agg_df = df.pivot_table(index=group_cols, values=agg_cols, aggfunc=aggregate_function).reset_index()

        # https://stackoverflow.com/questions/34049618/how-to-add-a-suffix-or-prefix-to-each-column-name
        # Why won't .add_prefix or .add_suffix work?
        if rename_cols:
            for c in agg_cols:
                agg_df = agg_df.rename(columns={c: f"{c}_{aggregate_function}"})

        final_df = pd.merge(final_df, agg_df, on=group_cols, how="left", validate="1:1")

        return final_df

    if len(sum_cols) > 0:
        final_df = aggregate_and_merge(df, final_df, group_cols, sum_cols, "sum")

    if len(mean_cols) > 0:
        final_df = aggregate_and_merge(df, final_df, group_cols, mean_cols, "mean")

    if len(count_cols) > 0:
        final_df = aggregate_and_merge(df, final_df, group_cols, count_cols, "count")

    if len(nunique_cols) > 0:
        final_df = aggregate_and_merge(df, final_df, group_cols, nunique_cols, "nunique")

    return final_df.drop(columns="index")
