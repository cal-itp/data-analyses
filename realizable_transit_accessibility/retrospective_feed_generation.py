from gtfslite import GTFS
from gtfs_utils import (
    copy_GTFS,
    time_string_to_time_since_midnight,
    seconds_to_gtfs_format_time,
)
from constants import RT_COLUMN_RENAME_MAP
import pandas as pd
import numpy as np
import typing

ColumnId = str
ColumnName = typing.Hashable
ColumnMap = dict[ColumnId, ColumnName]


def _filter_non_rt_trips(
    rt_schedule_stop_times: pd.DataFrame,
    rt_column: ColumnName,
    trip_instance_key_column: ColumnName,
    **_unused_column_names: ColumnMap
) -> pd.DataFrame:
    trips_by_rt_status = (
        rt_schedule_stop_times["rt_arrival_sec"]
        .isna()
        .groupby(rt_schedule_stop_times["trip_instance_key"])
        .all()
    )
    trips_without_rt = trips_by_rt_status[trips_by_rt_status].index
    filtered_stop_times = rt_schedule_stop_times.loc[
        ~(rt_schedule_stop_times["trip_instance_key"].isin(trips_without_rt))
    ].copy()
    return filtered_stop_times
    

def _filter_na_stop_times(
    rt_stop_times: pd.DataFrame,
    rt_column: ColumnName,
    **_unused_column_names: ColumnMap
) -> pd.DataFrame:
    return rt_stop_times.dropna(subset=[rt_column])


# THIS IS WRONG
"""
def flag_nonmonotonic_sections(
    rt_schedule_stop_times_sorted: pd.DataFrame,
    trip_instance_key_column: ColumnName,
    rt_column: ColumnName,
    schedule_column: ColumnName,
    **_unused_column_names: ColumnMap
) -> pd.DataFrame:
    rt_arrival_sec_shifted = rt_schedule_stop_times_sorted.groupby(
        trip_instance_key_column
    )[rt_column].shift(1)
    rt_arrival_sec_dips = (
        rt_arrival_sec_shifted > rt_schedule_stop_times_sorted[rt_column]
    ) & rt_schedule_stop_times_sorted[rt_column].notna()
    print(rt_arrival_sec_dips.any())
    return rt_arrival_sec_dips"""


def flag_nonmonotonic_sections(
    rt_schedule_stop_times_sorted: pd.DataFrame,
    trip_instance_key_column: ColumnName,
    rt_column: ColumnName,
    stop_sequence_column: ColumnName,
    **_unused_column_names: ColumnMap
) -> pd.DataFrame:
    assert not rt_schedule_stop_times_sorted.index.duplicated().any()
    rt_sec_reverse_cummin = (
        # Sort in reverse order
        rt_schedule_stop_times_sorted.sort_values(stop_sequence_column, ascending=False)
        # Get the minimum stop time in reverse order
        .groupby(trip_instance_key_column)[rt_column].cummin()
        # Reindex to undo the sort
        .reindex(rt_schedule_stop_times_sorted.index)
    )
    return (
        rt_sec_reverse_cummin != rt_schedule_stop_times_sorted[rt_column]
    ) & rt_schedule_stop_times_sorted[rt_column].notna()
    return nonmonotonic_flag


def add_monotonic_flag_to_df(
    rt_schedule_stop_times_sorted: pd.DataFrame,
    nonmonotonic_column: ColumnName,
    **column_name_args: ColumnMap
) -> pd.DataFrame:
    df_output = rt_schedule_stop_times_sorted.copy()
    df_output[nonmonotonic_column] = flag_nonmonotonic_sections(
        rt_schedule_stop_times_sorted, **column_name_args
    )
    return df_output


def impute_first_last(
    rt_schedule_stop_times_sorted: pd.DataFrame,
    trip_instance_key_column: ColumnName,
    rt_column: ColumnName,
    schedule_column: ColumnName,
    stop_sequence_column: ColumnName,
    nonmonotonic_column: ColumnName,
    **_unused_column_name_args: ColumnMap
) -> pd.DataFrame:
    assert not rt_schedule_stop_times_sorted[schedule_column].isna().any()
    # Get the first & last stop time in each trip
    stop_time_grouped = rt_schedule_stop_times_sorted.groupby(trip_instance_key_column)
    first_stop_time = stop_time_grouped.first()
    first_stop_sequence = first_stop_time[stop_sequence_column].rename(
        "first_stop_sequence"
    )
    last_stop_time = stop_time_grouped.last()
    last_stop_sequence = last_stop_time[stop_sequence_column].rename(
        "last_stop_sequence"
    )
    # Get the first / last stop time with RT data that is not the first/last stop time overall (resp.)
    # We need this to have a baseline to impute the first/last stop times
    stop_times_with_first_last_sequence = rt_schedule_stop_times_sorted.merge(
        pd.concat([first_stop_sequence, last_stop_sequence], axis=1),
        on=trip_instance_key_column,
        how="left",
        validate="many_to_one",
    )
    stop_times_na_dropped = stop_times_with_first_last_sequence.loc[
        stop_times_with_first_last_sequence[rt_column].notna()
        & ~stop_times_with_first_last_sequence[nonmonotonic_column]
    ]
    # Get the "second" stop time
    second_candidates = stop_times_na_dropped[
        stop_times_na_dropped[stop_sequence_column]
        > stop_times_na_dropped["first_stop_sequence"]
    ]
    second_stop_time = second_candidates.groupby(trip_instance_key_column).first()
    # Get the "penultimate" stop time
    penultimate_candidates = stop_times_na_dropped[
        stop_times_na_dropped[stop_sequence_column]
        < stop_times_na_dropped["last_stop_sequence"]
    ]
    penultimate_stop_time = penultimate_candidates.groupby(
        trip_instance_key_column
    ).last()
    # Get the scheduled time between first & "second" and "penultimate" & last stop
    scheduled_first_second_difference = (
        second_stop_time[schedule_column] - first_stop_time[schedule_column]
    )
    scheduled_penultimate_last_difference = (
        last_stop_time[schedule_column] - penultimate_stop_time[schedule_column]
    )

    assert (
        scheduled_first_second_difference.isna()
        | (scheduled_first_second_difference > 0)
    ).all()
    assert (
        scheduled_penultimate_last_difference.isna()
        | (scheduled_penultimate_last_difference > 0)
    ).all()
    rt_first_imputed = (
        second_stop_time[rt_column] - scheduled_first_second_difference
    ).rename("first_arrival_sec_imputed")
    rt_last_imputed = (
        penultimate_stop_time["rt_arrival_sec"] + scheduled_penultimate_last_difference
    ).rename("last_arrival_sec_imputed")
    # Merge in imputed first times
    stop_times_imputed_merged = stop_times_with_first_last_sequence.merge(
        pd.concat([rt_first_imputed, rt_last_imputed], axis=1),
        how="left",
        left_on="trip_instance_key",
        right_index=True,
        validate="many_to_one",
    )
    # Combine imputed and rt columns
    stop_times_imputed_merged["imputed_arrival_sec"] = (
        stop_times_imputed_merged["rt_arrival_sec"]
        .where(
            (
                stop_times_imputed_merged["first_stop_sequence"]
                != stop_times_imputed_merged["stop_sequence"]
            ),
            stop_times_imputed_merged["first_arrival_sec_imputed"],
        )
        .where(
            (
                stop_times_with_first_last_sequence["last_stop_sequence"]
                != stop_times_with_first_last_sequence["stop_sequence"]
            ),
            stop_times_imputed_merged["last_arrival_sec_imputed"],
        )
    )
    return stop_times_imputed_merged.drop(
        [
            "first_arrival_sec_imputed",
            "last_arrival_sec_imputed",
            "first_stop_sequence",
            "last_stop_sequence",
        ],
        axis=1,
    )


def impute_labeled_times(
    rt_schedule_stop_times_sorted: pd.DataFrame,
    rt_column: ColumnName,
    schedule_column: ColumnName,
    impute_label_column: ColumnName,
    trip_instance_key_column: ColumnName,
    **_unused_column_names: ColumnMap
) -> pd.Series:
    grouped_flag = rt_schedule_stop_times_sorted.groupby(trip_instance_key_column)[
        impute_label_column
    ]
    before_impute_group = (
        grouped_flag.shift(-1) & ~rt_schedule_stop_times_sorted[impute_label_column]
    )
    after_impute_group = (
        grouped_flag.shift(1) & ~rt_schedule_stop_times_sorted[impute_label_column]
    )
    # Get the schedule time at the last instance of before_impute_group and the first instance of after_impute_group
    before_time_schedule = rt_schedule_stop_times_sorted.loc[
        before_impute_group, schedule_column
    ].reindex(rt_schedule_stop_times_sorted.index, method="ffill")
    after_time_schedule = rt_schedule_stop_times_sorted.loc[
        after_impute_group, schedule_column
    ].reindex(rt_schedule_stop_times_sorted.index, method="bfill")
    # Get the rt time at the last instance of before_impute_group and the first instance of after_impute_group
    before_time_rt = rt_schedule_stop_times_sorted.loc[
        before_impute_group, rt_column
    ].reindex(rt_schedule_stop_times_sorted.index, method="ffill")
    after_time_rt = rt_schedule_stop_times_sorted.loc[
        after_impute_group, rt_column
    ].reindex(rt_schedule_stop_times_sorted.index, method="bfill")
    # Get the time passed in the schedule and rt feeds before and after impute sections
    before_after_schedule_difference = after_time_schedule - before_time_schedule
    before_after_rt_difference = after_time_rt - before_time_rt
    rt_schedule_proportion = (
        before_after_rt_difference / before_after_schedule_difference
    )
    # Get the difference between the current rt time and the next scheduled time
    imputed_difference = (
        rt_schedule_stop_times_sorted[schedule_column] - before_time_schedule
    ) * rt_schedule_proportion
    imputed_time = imputed_difference + before_time_rt
    merged_imputed_time = (
        rt_schedule_stop_times_sorted[rt_column]
        .where(~rt_schedule_stop_times_sorted[impute_label_column], imputed_time)
        .round()
    )
    return merged_imputed_time


def impute_non_monotonic_rt_times(
    rt_schedule_stop_times_sorted: pd.DataFrame,
    rt_column: ColumnName,
    schedule_column: ColumnName,
    nonmonotonic_column: ColumnName,
    trip_instance_key_column: ColumnName,
    **_unused_column_names: ColumnMap
):
    # Check that first/last trip times are present
    trip_id_grouped = rt_schedule_stop_times_sorted.groupby(trip_instance_key_column)
    assert not trip_id_grouped[rt_column].first().isna().any()
    assert not trip_id_grouped[rt_column].last().isna().any()
    # Check that schedule values are present for all trips
    assert not rt_schedule_stop_times_sorted[schedule_column].isna().any()
    # Check that the first and last values of each trip are not marked as nonmonotonic
    assert not trip_id_grouped[nonmonotonic_column].first().any()
    assert not trip_id_grouped[nonmonotonic_column].last().any()

    return impute_labeled_times(
        rt_schedule_stop_times_sorted,
        impute_label_column=nonmonotonic_column,
        rt_column=rt_column,
        nonmonotonic_column=nonmonotonic_column,
        schedule_column=schedule_column,
        trip_instance_key_column=trip_instance_key_column,
    )


def impute_short_gaps(
    rt_schedule_stop_times_sorted: pd.DataFrame,
    max_gap_length: int,
    rt_column: ColumnName,
    schedule_column: ColumnName,
    nonmonotonic_column: ColumnName,
    trip_instance_key_column: ColumnName,
    **_unused_column_names: ColumnMap
):
    # Check that first/last rt times are present
    trip_id_grouped = rt_schedule_stop_times_sorted.groupby(trip_instance_key_column)
    assert not trip_id_grouped[rt_column].first().isna().any()
    assert not trip_id_grouped[rt_column].last().isna().any()

    # Tag sections where there is a gap
    gap_present = rt_schedule_stop_times_sorted[rt_column].isna()
    gap_length = gap_present.groupby((~gap_present).cumsum()).transform("sum")
    imputable_gap_present = gap_present & (gap_length <= max_gap_length)
    print("imputable gap", imputable_gap_present.any())
    stop_times_copy = rt_schedule_stop_times_sorted.copy()
    stop_times_copy["impute"] = imputable_gap_present
    print("impute", stop_times_copy["impute"].any())
    print(imputable_gap_present)
    return impute_labeled_times(
        stop_times_copy,
        rt_column=rt_column,
        schedule_column=schedule_column,
        impute_label_column="impute",
        trip_instance_key_column=trip_instance_key_column,
    )


def make_retrospective_feed_single_date(
    filtered_input_feed: GTFS,
    stop_times_table: pd.DataFrame,
    stop_times_desired_columns: list[str],
    schedule_column: ColumnName,
    rt_column: ColumnName,
    trip_id_column: ColumnName,
    stop_id_column: ColumnName,
    stop_sequence_column: ColumnName,
    validate: bool = True,
    **_unused_column_names: ColumnMap
) -> GTFS:

    # Process the input feed
    schedule_trips_original = filtered_input_feed.trips.set_index("trip_id")
    schedule_stop_times_original = filtered_input_feed.stop_times.copy()
    schedule_stop_times_original["feed_arrival_sec"] = (
        time_string_to_time_since_midnight(schedule_stop_times_original["arrival_time"])
    )

    # Merge the schedule and rt stop time tables
    rt_trip_ids = stop_times_table[trip_id_column].drop_duplicates(keep="first")
    schedule_trips_in_rt = schedule_trips_original.loc[rt_trip_ids]
    stop_times_merged = schedule_stop_times_original.merge(
        stop_times_table.rename(
            columns={
                stop_id_column: "warehouse_stop_id",
                schedule_column: "warehouse_scheduled_arrival_sec",
            }
        ),
        left_on=["trip_id", "stop_sequence"],
        right_on=[trip_id_column, stop_sequence_column],
        how="left",  # TODO: left for proof of concept to simplify, should be outer
        validate="one_to_one",
    )

    if validate:
        # Validation
        # Stop ids match or are na
        assert (
            (stop_times_merged["stop_id"] == stop_times_merged["warehouse_stop_id"])
            | stop_times_merged["warehouse_stop_id"].isna()
        ).all()
        # Departure / arrival times match or are na
        assert (
            (
                stop_times_merged["feed_arrival_sec"]
                == stop_times_merged["warehouse_scheduled_arrival_sec"]
            )
            | stop_times_merged["feed_arrival_sec"].isna()
            | stop_times_merged["warehouse_scheduled_arrival_sec"].isna()
        ).all()
        # All RT stop times have an arrival sec
        assert (
            ~stop_times_merged["feed_arrival_sec"].isna()
            | stop_times_merged[
                "schedule_gtfs_dataset_key"
            ].isna()  # TODO: should be a constant
        ).all()

    stop_times_merged_filtered = stop_times_merged.loc[
        ~stop_times_merged["schedule_gtfs_dataset_key"].isna()
    ].reset_index(drop=True)
    stop_times_merged_filtered["rt_arrival_gtfs_time"] = seconds_to_gtfs_format_time(
        stop_times_merged_filtered[rt_column]
    )
    stop_times_gtfs_format_with_rt_times = (
        stop_times_merged_filtered.drop(["arrival_time", "departure_time"], axis=1)
        .rename(
            columns={
                "rt_arrival_gtfs_time": "arrival_time",
            }
        )[
            np.intersect1d(
                stop_times_desired_columns, stop_times_merged_filtered.columns
            )
        ]
        .copy()
    )
    # TODO: not sure if this is the correct thing to do, for first/last trips
    # TODO: move this earlier on, so departure_time ends up in the desired position in columns
    stop_times_gtfs_format_with_rt_times["departure_time"] = (
        stop_times_gtfs_format_with_rt_times["arrival_time"].copy()
    )

    # Output a new synthetic feed!
    # Alter the feed with the new trips and stop times
    altered_feed = copy_GTFS(filtered_input_feed)
    altered_feed.trips = schedule_trips_in_rt.reset_index()
    altered_feed.stop_times = stop_times_gtfs_format_with_rt_times

    # Not sure if this is appropriate or not, since we're altering. Leaving commented out for now
    # Possibly should go in subset_schedule_feed_to_one_date
    """
    new_feed_info = pd.DataFrame({
        "feed_publisher_name": "California Department of Transportation",
        "feed_publisher_url": "https://dot.ca.gov",
        "feed_lang": np.nan if altered_feed.feed_info is not None else altered_feed.feed_info["feed_lang"].iloc[0],
        "feed_start_date": SAMPLE_DATE_STR,
        "feed_end_date": SAMPLE_DATE_STR,
        "feed_version": f"retrospective_{SAMPLE_DATE_STR}" if altered_feed.feed_info is not None else  f"retrospective_{altered_feed.feed_info["feed_version"]}_{SAMPLE_DATE_STR}"
    })
    """
    # Copy the feed - this is necessary to validate the feed meets the standard since gtfs-lite only validates feeds on creation
    output_feed = copy_GTFS(altered_feed)
    return output_feed
