"""
Standardize operator level naming for reports.

With GTFS data, analysis is done with identifiers (calitp_itp_id, route_id, etc).
When publishing reports, we should be referring to operator name,
route name, Caltrans district the same way.

Based on `rt_delay/speedmaps.ipynb`,
`bus_service_increase/competitive-parallel-routes.ipynb`
"""

import re
from datetime import date, timedelta

from calitp.tables import tbl
from siuba import *


def add_agency_name():
    # This is the agency_name used in RT maps
    # rt_delay/rt_analysis.py#L309
    df = (
        (
            tbl.views.gtfs_schedule_dim_feeds()
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
def add_caltrans_district():
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

    return df


# https://github.com/cal-itp/data-analyses/blob/main/rt_delay/utils.py
def add_route_name(SELECTED_DATE=date.today() + timedelta(days=-1)):
    """
    SELECTED_DATE: datetime or str.
        Defaults to yesterday's date.
        Should match the date of the analysis.
    """

    route_names = (
        tbl.views.gtfs_schedule_dim_routes()
        >> filter(
            _.calitp_extracted_at < SELECTED_DATE, _.calitp_deleted_at >= SELECTED_DATE
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

    def exclude_desc(desc):
        # match descriptions that don't give additional info, like Route 602 or Route 51B
        exclude_texts = [
            " *Route *[0-9]*[a-z]{0,1}$",
            " *Metro.*(Local|Rapid|Limited).*Line",
            " *(Redwood Transit serves the communities of|is operated by Eureka Transit and serves)",
            " *service within the Stockton Metropolitan Area",
            " *Hopper bus can deviate",
            " *RTD's Interregional Commuter Service is a limited-capacity service",
        ]
        desc_eval = [
            re.search(text, desc, flags=re.IGNORECASE) for text in exclude_texts
        ]
        # number_only = re.search(' *Route *[0-9]*[a-z]{0,1}$', desc, flags=re.IGNORECASE)
        # metro = re.search(' *Metro.*(Local|Rapid|Limited).*Line', desc, flags=re.IGNORECASE)
        # redwood = re.search(' *(Redwood Transit serves the communities of|is operated by Eureka Transit and serves)', desc, flags=re.IGNORECASE)
        # return number_only or metro or redwood
        return any(desc_eval)

    def which_desc(row):
        long_name_valid = row.route_long_name and not exclude_desc(row.route_long_name)
        route_desc_valid = row.route_desc and not exclude_desc(row.route_desc)
        if route_desc_valid:
            return row.route_desc.title()
        elif long_name_valid:
            return row.route_long_name.title()
        else:
            return ""

    route_names = route_names.assign(
        route_name_used=route_names.apply(lambda x: which_desc(x), axis=1)
    )

    # Just keep important columns to merge
    keep_cols = ["calitp_itp_id", "route_id", "route_name_used"]
    route_names = route_names[keep_cols].sort_values(keep_cols).reset_index(drop=True)

    return route_names


# https://github.com/cal-itp/data-analyses/blob/main/traffic_ops/prep_data.py
def latest_itp_id():
    df = (
        tbl.views.gtfs_schedule_dim_feeds()
        >> filter(_.calitp_id_in_latest is True)
        >> select(_.calitp_itp_id)
        >> distinct()
        >> collect()
    )

    return df
