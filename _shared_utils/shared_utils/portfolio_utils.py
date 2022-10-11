"""
Common functions for standardizing how outputs are displayed in portfolio.

1. Standardize operator level naming for reports.
With GTFS data, analysis is done with identifiers (calitp_itp_id, route_id, etc).
When publishing reports, we should be referring to operator name,
route name, Caltrans district the same way.

2. Table styling for pandas styler objects

"""
import datetime as dt

import pandas as pd
import pandas.io.formats.style
from calitp.tables import tbl
from IPython.display import HTML
from shared_utils import rt_utils
from siuba import *

# need to import different pandas to add type hint for styler object (https://github.com/pandas-dev/pandas/issues/24884)


def add_agency_name(
    selected_date: str | dt.date = dt.date.today() + dt.timedelta(days=-1),
) -> pd.DataFrame:
    """
    Returns a dataframe with calitp_itp_id and the agency name used in portfolio.

    This is the agency_name used in RT maps
    rt_delay/rt_analysis.py#L309
    """
    df = (
        (
            tbl.views.gtfs_schedule_dim_feeds()
            >> filter(
                _.calitp_extracted_at < selected_date,
                _.calitp_deleted_at >= selected_date,
            )
            >> select(_.calitp_itp_id, _.calitp_agency_name)
            >> distinct()
            >> collect()
        )
        .sort_values(["calitp_itp_id", "calitp_agency_name"])
        .drop_duplicates(subset="calitp_itp_id")
        .reset_index(drop=True)
    )

    return df


# https://github.com/cal-itp/data-analyses/blob/main/bus_service_increase/E5_make_stripplot_data.py
def add_caltrans_district() -> pd.DataFrame:
    """
    Returns a dataframe with calitp_itp_id and the caltrans district
    """
    df = (
        (
            tbl.airtable.california_transit_organizations()
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
def add_route_name(
    selected_date: str | dt.date = dt.date.today() + dt.timedelta(days=-1),
) -> pd.DataFrame:
    """
    selected_date: datetime or str.
        Defaults to yesterday's date.
        Should match the date of the analysis.
    """

    route_names = (
        tbl.views.gtfs_schedule_dim_routes()
        >> filter(
            _.calitp_extracted_at < selected_date, _.calitp_deleted_at >= selected_date
        )
        >> select(
            _.calitp_itp_id,
            _.route_id,
            _.route_short_name,
            _.route_long_name,
            _.route_desc,
        )
        >> distinct()
        >> collect()
    )

    route_names = route_names.assign(
        route_name_used=route_names.apply(lambda x: rt_utils.which_desc(x), axis=1)
    )

    # Just keep important columns to merge
    keep_cols = ["calitp_itp_id", "route_id", "route_name_used"]
    route_names = route_names[keep_cols].sort_values(keep_cols).reset_index(drop=True)

    # If route names show up with leading comma
    route_names = route_names.assign(
        route_name_used=route_names.route_name_used.str.lstrip(",").str.strip()
    )

    return route_names


# https://github.com/cal-itp/data-analyses/blob/main/traffic_ops/prep_data.py
def latest_itp_id() -> pd.DataFrame:
    """
    Returns a dataframe of 1 column with the latest calitp_itp_ids.
    """
    df = (
        tbl.views.gtfs_schedule_dim_feeds()
        >> filter(_.calitp_id_in_latest == True)
        >> select(_.calitp_itp_id)
        >> distinct()
        >> collect()
    )

    return df


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
) -> pd.io.formats.style.Styler | str:
    """
    Returns a pandas Styler object with some basic formatting.
    Any other tweaks for currency, percentages, etc should be done before / after.

    Generalize with dict comprehension or list comprehension
    list comprehension: df.style.format(subset=percent_cols,  **{'formatter': '{:,.2%}'})
    dict comprehension: df.style.format(formatter = {c: '{:,.2%}' for c in percent_cols})
    """
    df = df.drop(columns=drop_cols).rename(columns=rename_cols)

    if len(integer_cols) > 0:
        df = df.astype({c: "Int64" for c in integer_cols})

    if left_align_cols == "first":
        left_align_cols = list(df.columns)[0]
    if center_align_cols == "all":
        center_align_cols = list(df.columns)
        # all other columns except first one is center aligned
        center_align_cols = [c for c in center_align_cols if c not in left_align_cols]

    df_style = (
        df.style.format(formatter={c: "{:,g}" for c in integer_cols})
        .format(formatter={c: "{:,.1f}" for c in one_decimal_cols})
        .format(formatter={c: "{:,.2f}" for c in two_decimal_cols})
        .format(formatter={c: "{:,.3f}" for c in three_decimal_cols})
        .format(formatter={c: "{:,.2%}" for c in percent_cols})
        .format(formatter={c: "$ {:,.2f}" for c in currency_cols})
        .set_properties(subset=left_align_cols, **{"text-align": "left"})
        .set_properties(subset=center_align_cols, **{"text-align": "center"})
        .set_properties(subset=right_align_cols, **{"text-align": "right"})
        .set_table_styles([dict(selector="th", props=[("text-align", "center")])])
        .hide(axis="index")
    )

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
        new_styler = df_style.format(formatter={c: format_str for c in cols_to_format})

        return new_styler

    if len(list(custom_format_cols.keys())) > 0:
        for format_str, cols_to_format in custom_format_cols.items():
            df_style = add_custom_format(df_style, format_str, cols_to_format)

    if display_table is True:
        display(HTML(df_style.to_html()))

    return df_style
