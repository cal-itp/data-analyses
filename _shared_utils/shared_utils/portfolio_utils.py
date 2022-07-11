"""
Standardize operator level naming for reports.

With GTFS data, analysis is done with identifiers (calitp_itp_id, route_id, etc).
When publishing reports, we should be referring to operator name,
route name, Caltrans district the same way.

Based on `rt_delay/speedmaps.ipynb`,
`bus_service_increase/competitive-parallel-routes.ipynb`
"""
from datetime import date, timedelta

from calitp.tables import tbl
from shared_utils import rt_utils
from siuba import *


def add_agency_name(SELECTED_DATE=date.today() + timedelta(days=-1)):
    # This is the agency_name used in RT maps
    # rt_delay/rt_analysis.py#L309
    # Similar version here with date needed - https://github.com/cal-itp/data-analyses/blob/main/high_quality_transit_areas/combine_and_visualize.ipynb
    df = (
        (
            tbl.views.gtfs_schedule_dim_feeds()
            >> filter(
                _.calitp_extracted_at < SELECTED_DATE,
                _.calitp_deleted_at >= SELECTED_DATE,
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

    route_names = route_names.assign(
        route_name_used=route_names.apply(lambda x: rt_utils.which_desc(x), axis=1)
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
