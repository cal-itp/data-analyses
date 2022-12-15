"""
Handle Muni weekend rail stops separately.
When Muni transitions new rail service away from weekend-only
service, just remove this script.

Differences from download_data.py:
* order of downloading is slightly, as we just need rail trips 
(subset by route_types) and don't need to cache a weekend trips table parquet.
* don't need all the stops, but need to grab stop_times to find
the stops listed for rail trips, and then get stop geom. 
this puts stop_times ahead of stops query, when usually, stop_times is not
downloaded unless trips, routelines, and stops is present.
"""
import pandas as pd
import pendulum

from shared_utils import utils, gtfs_utils, geography_utils, rt_utils
from update_vars import analysis_date, TEMP_GCS

date_str = analysis_date.strftime(rt_utils.FULL_DATE_FMT)

previous_sat = (pendulum.from_format(
                date_str, 
                fmt = "YYYY-MM-DD")
                .date()
                .previous(pendulum.SATURDAY)
                .strftime(rt_utils.FULL_DATE_FMT)
               )

# Muni weekend trips, instead of going from primary_trip_query, 
# subset it with route_type first, 
# then go to trip table, and this will prevent querying way too many rows
# the remainder, routelines, stops, stop_times can be done with gtfs_utils,
# but stop_times needs to be downloaded first before to find which stops are there
# this needs to be worked into A1, A2
def download_muni_weekend_rail_trips(
    itp_id: int, analysis_date: str, 
    additional_filters: dict = {"route_type": ['0', '1', '2']}
) -> pd.DataFrame: 
    """
    Download Muni trips table for rail on Saturday service.
    Temporarily do this, as these are new rail stops that just opened
    and are running weekend-only service for now.
    
    Update in 2023 when it transitions to weekday service.
    https://www.sfmta.com/blog/central-subway-opens-november-19-special-weekend-service
    """
    keep_route_cols = [
        "calitp_itp_id", 
        "route_id", "route_short_name", "route_long_name",
        "route_desc", "route_type"
    ]

    routes = gtfs_utils.get_route_info(
        selected_date = analysis_date,
        itp_id_list = [itp_id],
        route_cols = keep_route_cols,
        get_df = True, 
        custom_filtering = additional_filters
    )
    
    # Just grab the route_ids in the trip table
    subset_routes = routes.route_id.unique().tolist()
    
    keep_trip_cols = [
        "calitp_itp_id", "calitp_url_number", 
        "service_date", "trip_key", "trip_id",
        "route_id", "direction_id", "shape_id",
        "calitp_extracted_at", "calitp_deleted_at"
    ]

    subset_trips = gtfs_utils.get_trips(
        selected_date = analysis_date, 
        itp_id_list = [itp_id],
        trip_cols = keep_trip_cols,
        get_df = True,
        custom_filtering = {"route_id": subset_routes}
    )
    
    return subset_trips


def download_muni_stops(
    itp_id: int = 282, 
    analysis_date: str = previous_sat
):
    """
    Once the weekend rail trips are downloaded,
    see which stops appear in stop_times for those trips.
    Then attach geometry to stops.
    """
    
    muni_weekend_rail_trips = download_muni_weekend_rail_trips(
        itp_id, analysis_date, 
        additional_filters = {"route_type": ['0', '1', '2']}
    )
    
    muni_weekend_rail_trips.to_parquet(
        f"{TEMP_GCS}muni_weekend_rail_trips.parquet")
    
    muni_stop_times = gtfs_utils.get_stop_times(
        selected_date = analysis_date,
        itp_id_list = [itp_id],
        stop_time_cols = None,
        get_df = True,
        departure_hours = None,
        trip_df = muni_weekend_rail_trips
    )
    
    unique_muni_weekend_rail_stops = muni_stop_times.stop_id.unique().tolist()
    
    keep_stop_cols = [
        "calitp_itp_id", "stop_id", 
        "stop_lat", "stop_lon",
        "stop_name", "stop_key"
    ]

    muni_stops = (gtfs_utils.get_stops(
        selected_date = analysis_date,
        itp_id_list = [itp_id],
        stop_cols = keep_stop_cols,
        get_df = True,
        crs = geography_utils.CA_NAD83Albers,
        custom_filtering = {"stop_id": unique_muni_weekend_rail_stops}
        ).drop_duplicates(subset=["calitp_itp_id", "stop_id"])
        .reset_index(drop=True)
    )
    
    utils.geoparquet_gcs_export(
        muni_stops, 
        TEMP_GCS,
        "muni_rail_stops"
    )
 
    return muni_stops
