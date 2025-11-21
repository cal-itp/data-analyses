import datetime as dt

import pandas as pd
import yaml
from segment_speed_utils import gtfs_schedule_wrangling, helpers


def read_published_operators(
    current_date: str, published_operators_yaml: str = "../gtfs_funnel/published_operators.yml", lookback_days=95
):

    # Read in the published operators file
    with open(published_operators_yaml) as f:
        published_operators_dict = yaml.safe_load(f)

    current_date = dt.date.fromisoformat(current_date)
    lookback_limit = current_date - dt.timedelta(days=lookback_days)

    # Convert the published operators file into a dict mapping dates to an iterable of operators
    patch_operators_dict = {
        str(key): published_operators_dict[key]
        for key in published_operators_dict.keys()
        if key > lookback_limit and key < current_date
    }

    return patch_operators_dict


def get_lookback_trips(published_operators_dict: dict, trips_cols: list) -> pd.DataFrame:
    """
    Get trips according to published_operators_dict.
    Trips reflect the most recent date each operator appeared.
    """
    lookback_trips = []
    for date in published_operators_dict.keys():
        lookback_trips += [
            helpers.import_scheduled_trips(
                date, filters=[["name", "in", published_operators_dict[date]]], columns=trips_cols
            ).assign(analysis_date=date)
        ]
    return pd.concat(lookback_trips)


def lookback_trips_ix(lookback_trips: pd.DataFrame) -> pd.DataFrame:
    """
    Keep identifier cols and drop duplicates, use for filtering in other lookback
    functions.
    """
    lookback_trips_ix = lookback_trips[
        ["name", "feed_key", "schedule_gtfs_dataset_key", "analysis_date"]
    ].drop_duplicates()
    return lookback_trips_ix


def get_lookback_st(published_operators_dict: dict, lookback_trips_ix: pd.DataFrame, st_cols: list) -> pd.DataFrame:
    """
    Get stop_times according to published_operators_dict.
    stop_times reflect the most recent date each operator appeared.
    """
    lookback_st = []
    assert "feed_key" in st_cols, "must include feed key for filtering"
    for date in published_operators_dict.keys():
        feed_keys = lookback_trips_ix.query("analysis_date == @date").feed_key.unique()
        st = helpers.import_scheduled_stop_times(
            date,
            columns=st_cols,
            filters=[["feed_key", "in", feed_keys]],
            with_direction=False,  # required to include rail/ferry/brt stops w/out shapes
            get_pandas=True,
        )
        # display(st)
        lookback_st += [st]
    return pd.concat(lookback_st)


def get_lookback_stops(published_operators_dict: dict, lookback_trips_ix: pd.DataFrame, stops_cols: list, **kwargs):
    """
    Get stops according to published_operators_dict.
    stops reflect the most recent date each operator appeared.
    """
    lookback_stops = []
    assert "feed_key" in stops_cols, "must include feed key for filtering"
    for date in published_operators_dict.keys():
        feed_keys = lookback_trips_ix.query("analysis_date == @date").feed_key.unique()
        stops = helpers.import_scheduled_stops(
            date, columns=stops_cols, filters=[["feed_key", "in", feed_keys]], get_pandas=True, **kwargs
        ).assign(analysis_date=date)
        # display(st)
        lookback_stops += [stops]
    return pd.concat(lookback_stops)


def get_lookback_hqta_shapes(published_operators_dict, lookback_trips_ix, no_drop=False):
    """
    Get shapes according to published_operators_dict.
    Shapes reflect the most recent date each operator appeared,
    and additionally implement the same processing steps as current shapes
    """
    lookback_shapes = []
    for date in published_operators_dict.keys():
        feed_keys = lookback_trips_ix.query("analysis_date == @date").feed_key.unique()  # noqa: F841
        # Only include certain Amtrak routes
        outside_amtrak_shapes = gtfs_schedule_wrangling.amtrak_trips(  # noqa: F841
            analysis_date=date, inside_ca=False
        ).shape_array_key.unique()

        gdf = (
            gtfs_schedule_wrangling.longest_shape_by_route_direction(analysis_date=date)
            .query("shape_array_key not in @outside_amtrak_shapes & feed_key.isin(@feed_keys)")
            .fillna({"direction_id": 0})
            .astype({"direction_id": "int"})
            .assign(analysis_date=date)
        )
        if not no_drop:
            gdf = gdf.drop(columns=["feed_key", "shape_array_key", "route_length"])
        lookback_shapes += [gdf]
    return pd.concat(lookback_shapes)
