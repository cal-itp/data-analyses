from functools import cache

from calitp_data_analysis.gcs_geopandas import GCSGeoPandas
from calitp_data_analysis.gcs_pandas import GCSPandas
from shared_utils import rt_utils
from tqdm import tqdm
from update_vars import GCS_PATH, GEOM_SUBFOLDER

tqdm.pandas(desc="Progress")


@cache
def gcs_pandas():
    return GCSPandas()


@cache
def gcs_geopandas():
    return GCSGeoPandas()


def read_shapes_stopping_in_seg(analysis_date):
    # cols = ['shape_array_key', 'tsi_segment_id']
    sstb = gcs_pandas().read_parquet(f"{GCS_PATH}{GEOM_SUBFOLDER}shape_stops_areas_borders_{analysis_date}.parquet")
    sstb["has_stop"] = True
    return sstb


def attach_stopping_info(trip_segment_df, shape_stopping_df):
    """ """
    df = trip_segment_df.merge(shape_stopping_df, how="left", on=["shape_array_key", "tsi_segment_id"])
    df.has_stop = df.has_stop.convert_dtypes().fillna(False)
    return df


def locate_stopping_segments(row, df):
    if row.has_stop:
        return row
    else:
        id_before = None
        id_after = None
        print(row.name)
        stop_before = df.loc[: (row.name - 1)].query("has_stop")
        if not stop_before.empty:
            id_before = stop_before.query("start_meters == start_meters.max()").tsi_segment_id.iloc[0]
        stop_after = df.loc[(row.name + 1) :].query("has_stop")
        if not stop_after.empty:
            id_after = stop_after.query("start_meters == start_meters.min()").tsi_segment_id.iloc[0]
        row["stopping_segments"] = (id_before, id_after)
        # return (id_before, id_after)
        return row


def assign_stopping_sequences(joined_df):
    """
    a joined trip tsi segment df and shape
    stopping df, create a new df by shape showing
    which tsi segments (tracts or border zones) vrh & vrm
    should be allocated to when there are no stops for that
    shape in that segment
    """
    cols = ["shape_array_key", "start_meters", "tsi_segment_id", "has_stop"]
    simple_sequence_df = (
        joined_df[cols].drop_duplicates().sort_values(["shape_array_key", "start_meters"]).reset_index(drop=True)
    )
    #  hacky but need both the individual row and to scan the whole dataframe
    fn = lambda df: df.apply(locate_stopping_segments, df=df, axis=1)  # noqa: E731
    #  tuples will be (None, id) where there are no previous stops, or (id, None) where no subsequent stops
    stopping_sequences_df = simple_sequence_df.groupby(["shape_array_key"], group_keys=False).progress_apply(fn)
    #  scrub nones from tuples for accurate count:
    stopping_sequences_df.stopping_segments = stopping_sequences_df.stopping_segments.map(
        lambda y: y if not type(y).isinstance(tuple) else tuple(x for x in y if x)
    )
    stopping_sequences_df["n_stopping_segments"] = stopping_sequences_df.stopping_segments.map(
        lambda y: y if not type(y).isinstance(tuple) else len(y)
    ).fillna(1)
    unassigned = stopping_sequences_df.query("n_stopping_segments == 0")
    print(f"{unassigned.shape[0]} segments out of {stopping_sequences_df.shape[0]} can not be matched to a stop")
    stopping_sequences_df = stopping_sequences_df.query("n_stopping_segments >= 1")
    #  divide time and distance in tsi segments by number of segments post-explode
    joined_df = joined_df.merge(
        stopping_sequences_df, on=["has_stop", "shape_array_key", "start_meters", "tsi_segment_id"]
    ).explode("stopping_segments")
    joined_df = joined_df.assign(
        tsi_segment_meters=joined_df.tsi_segment_meters / joined_df.n_stopping_segments,
        segment_seconds=joined_df.segment_seconds / joined_df.n_stopping_segments,
    )
    #  replace tsi_segment_id with stopping_segment if present, df can now be aggregated normally on tsi_segment_id
    joined_df.tsi_segment_id = joined_df.stopping_segments.fillna(joined_df.tsi_segment_id)
    joined_df = joined_df.drop(
        columns=[
            "has_stop",
            "arrival_sec",
            "arrival_sec_next",
            "start_meters",
            "stopping_segments",
            "n_stopping_segments",
        ]
    )
    return joined_df


def assign_borders(stopping_sequences_df, border_df, id_col="tract"):
    """ """
    border_cols = ["tsi_segment_id", "border_areas", "border"]
    border_df = border_df.assign(border_areas=tuple(zip(border_df[f"{id_col}_1"], border_df[f"{id_col}_2"])))[
        border_cols
    ].drop_duplicates()
    border_df.border_areas = border_df.border_areas.map(lambda x: None if x == (None, None) else x)
    border_merged = stopping_sequences_df.merge(border_df, how="left", on="tsi_segment_id")
    border_merged["border_divide"] = border_merged.border.replace({True: 2, False: 1})
    border_merged = border_merged.explode("border_areas")
    border_merged = border_merged.assign(
        tsi_segment_meters=border_merged.tsi_segment_meters / border_merged.border_divide,
        segment_seconds=border_merged.segment_seconds / border_merged.border_divide,
    )
    border_merged[id_col] = border_merged.border_areas.fillna(border_merged.tsi_segment_id)
    border_merged = border_merged.drop(columns=["border_divide", "border_areas"])
    return border_merged


def aggregate_to_area(border_assigned_df, group_cols=["tract"]):
    """ """
    sum_cols = ["tsi_segment_meters", "segment_seconds"]
    grouped = border_assigned_df.groupby(group_cols)[sum_cols]
    aggregated = grouped.sum().reset_index()
    aggregated = aggregated.assign(
        daily_vrm_miles=aggregated.tsi_segment_meters / rt_utils.METERS_PER_MILE,
        daily_vrh_hours=aggregated.segment_seconds / 60**2,
    )
    aggregated = aggregated.drop(columns=sum_cols)
    return aggregated.round(1)
