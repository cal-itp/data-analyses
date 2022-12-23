"""
Migrate gtfs_utils from v1 to v2 warehouse.

Changes from v1 to v2:
1. new feed_key to organization_name table created.
    Should probably stage in dbt anyway to see what feeds / names
    are there for the same day?
    Rather than subsetting by feed_keys, join this new table to
    all subsequent queries, so analysts can query by org name too.

2. get_trips: expand and move to dbt
    Rework trips query to better reflect how we use this query as our
    base query.
    Bring in route columns we might use to subset
    (route_type, route_short_name, route_long_name).

    Move this joining into dbt to be a mart_trips table that analysts go to first.

3. get_route_info: deprecate.
    Instead, expand the trips query to incorporate our most-used
    route columns from dim_routes to help with subsetting. See #2

4. get_route_shapes: change make_routes_gdf to take pd.DataFrame,
    keep in gtfs_utils_v2
    Use dask.apply to apply the make_linestring function.
    Move this new make_routes_gdf over to geography_utils when it's ready
    and replace that.

5. get_stops: move to dbt
    set this up so that point geometry can be created in dbt.
    Add a mart_stops table that analysts go to first.

6. get_stop_times: TODO
    TODO. think more on what functions typically come from stop_times.
    Aggregation, for sure, and that can be handled if it returns dask.dataframe
    as in previous get_stop_times.

    With new timestamp that's numeric, we might want to add subsetting by departure
    hour on top of that so we don't have to do the calculation of seconds to
    departure hour more.

    But are these common use cases enough to justify a new dbt table that
    acts as mart_stop_times? Leaning towards no for now...

"""

# import os
# os.environ["CALITP_BQ_MAX_BYTES"] = str(200_000_000_000)

import datetime
from typing import Union

import dask.dataframe as dd

# import dask_geopandas as dg
import geopandas as gpd
import pandas as pd
import siuba  # need this to do type hint in functions
from calitp.tables import tbls
from shared_utils import geography_utils
from siuba import *

GCS_PROJECT = "cal-itp-data-infra"

# YESTERDAY_DATE = datetime.date.today() + datetime.timedelta(days=-1)


def daily_feed_to_organization(
    selected_date: Union[str, datetime.date], get_df: bool = True
) -> Union[pd.DataFrame, siuba.sql.verbs.LazyTbl]:
    """
    Select a date, find what feeds are present, and
    merge in organization name.
    """
    dim_gtfs_datasets = (
        tbls.mart_transit_database.dim_gtfs_datasets()
        >> filter(_["data"] == "GTFS Schedule")
        >> rename(gtfs_dataset_key="key")
        >> select(_.gtfs_dataset_key, _.name, _.regional_feed_type)
        >> distinct()
    )

    fact_feeds = (
        tbls.mart_gtfs.fct_daily_schedule_feeds()
        >> filter((_.date == selected_date))
        >> inner_join(_, dim_gtfs_datasets, on="gtfs_dataset_key")
    )

    if get_df:
        fact_feeds = fact_feeds >> collect()

    return fact_feeds


def get_trips(
    selected_date: Union[str, datetime.date],
    subset_feeds: list = None,
    get_df: bool = True,
) -> Union[pd.DataFrame, siuba.sql.verbs.LazyTbl]:
    """
    Modify this for dbt so that we always have route_info query too.

    TODO: metrolink fix.
    """
    trips = (
        tbls.mart_gtfs.fct_daily_scheduled_trips()
        >> filter(_.service_date == selected_date)
        >> filter(_.feed_key.isin(subset_feeds))
        >> inner_join(
            _,
            (tbls.mart_gtfs.dim_trips() >> rename(trip_key="key")),
            on="trip_key",
        )
        >> inner_join(
            _,
            (
                tbls.mart_gfs.dim_routes()
                >> rename(route_key="key")
                >> select(
                    _.route_key,
                    _.route_type,
                    _.route_short_name,
                    _.route_long_name,
                    _.route_desc,
                )
            ),
            on="route_key",
        )
        >> distinct()
    )

    if get_df:
        trips = trips >> collect()

    return trips


def get_route_info(
    selected_date: Union[str, datetime.date],
    subset_feeds: list = None,
    get_df: bool = True,
) -> Union[pd.DataFrame, siuba.sql.verbs.LazyTbl]:
    """
    Want to deprecate this one in v2...bring in columns needed for routes
    into the expanded trips query. give as much flexibility for
    subsetting there.
    """
    routes = (
        tbls.mart_gtfs.fct_daily_scheduled_trips()
        >> filter(_.service_date == selected_date)
        >> filter(_.feed_key.isin(subset_feeds))
        >> select(_.route_key, _.service_date)
        >> distinct()
        >> inner_join(
            _, (tbls.mart_gtfs.dim_routes() >> rename(route_key="key")), on="route_key"
        )
        >> distinct()
    )

    if get_df:
        routes = routes >> collect()

    return routes


def make_routes_gdf_NEW(
    df: pd.DataFrame,
    crs: str = "EPSG:4326",
) -> gpd.GeoDataFrame:
    """
    Parameters:

    crs: str, a projected coordinate reference system.
        Defaults to EPSG:4326 (WGS84)
    """
    # Use apply to use map_partitions
    # https://stackoverflow.com/questions/70829491/dask-map-partitions-meta-when-using-lambda-function-to-add-column
    ddf = dd.from_pandas(df, npartitions=1)

    ddf["geometry"] = ddf.pt_array.apply(
        geography_utils.make_linestring, meta=("geometry", "geometry")
    )
    shapes = ddf.compute()

    # apply the function
    # df["geometry"] = df.pt_array.apply(geography_utils.make_linestring)

    # convert to geopandas; geometry column contains the linestring, re-project if needed
    gdf = gpd.GeoDataFrame(
        shapes.drop(columns="pt_array"), geometry="geometry", crs=geography_utils.WGS84
    ).to_crs(crs)

    return gdf


def get_route_shapes(
    selected_date: Union[str, datetime.date],
    subset_feeds: list = None,
    get_df: bool = True,
    crs: str = geography_utils.WGS84,
) -> gpd.GeoDataFrame:
    """
    TODO: move make_routes_gdf back to geography_utils later
    but this needs a pd.DataFrame going in...so keep it here for now
    """

    route_shapes = (
        tbls.mart_gtfs.fct_daily_scheduled_trips()
        >> filter(_.service_date == selected_date)
        >> filter(_.feed_key.isin(subset_feeds))
        >> select(_.shape_array_key, _.service_date)
        >> inner_join(
            _,
            (tbls.mart_gtfs.dim_shapes_arrays() >> rename(shape_array_key="key")),
            on="shape_array_key",
        )
        >> collect()
    )

    shapes_gdf = make_routes_gdf_NEW(route_shapes, crs=crs)

    return shapes_gdf


def get_stops(
    selected_date: Union[str, datetime.date],
    subset_feeds: list = None,
    get_df: bool = True,
) -> Union[pd.DataFrame, siuba.sql.verbs.LazyTbl]:
    """ """
    stops = (
        tbls.mart_gtfs.fct_daily_scheduled_trips()
        >> filter(_.service_date == selected_date)
        >> filter(_.feed_key.isin(subset_feeds))
        >> select(_.stop_key, _.service_date)
        >> distinct()
        >> inner_join(
            _, (tbls.mart_gtfs.dim_stops() >> rename(stop_key="key")), on="stop_key"
        )
        >> distinct()
    )

    if get_df:
        stops = stops >> collect()
        stops = geography_utils.create_point_geometry(stops, crs=crs).drop(
            columns=["stop_lon", "stop_lat"]
        )

    return stops


# TODO: stop_times.
