"""
Functions related to publishing a set of datasets
in ESRI / ArcPro.

Sometimes we want to keep column names consistent across
our datasets so users know they refer to the same entity,
and other times the metadata and data dictionary is too
finicky to change often.
"""

import pandas as pd

STANDARDIZED_COLUMNS_DICT = {
    "agency_name_primary": "agency_primary",
    "agency_name_secondary": "agency_secondary",
    "route_short_name": "route_name",
    "portfolio_organization_name": "agency",
    "analysis_name": "agency",
    "time_of_day": "time_period",
    "caltrans_district": "district_name",
}


def standardize_column_names(df: pd.DataFrame) -> pd.DataFrame:
    """
    Standardize how agency is referred to.
    """
    return df.rename(columns=STANDARDIZED_COLUMNS_DICT)


def remove_internal_keys(df: pd.DataFrame) -> pd.DataFrame:
    """
    Remove columns used in our internal data modeling.
    Leave only natural identifiers (route_id, shape_id).
    Remove shape_array_key, gtfs_dataset_key, etc.
    """
    exclude_list = [
        "sec_elapsed",
        "meters_elapsed",
        "name",
        "schedule_gtfs_dataset_key",
        "source_record_id",
        "stop_pair",
    ]  # use segment_id instead, includes stop_ids
    cols = [c for c in df.columns]

    internal_cols = [c for c in cols if "_key" in c or c in exclude_list]

    print(f"drop: {internal_cols}")

    return df.drop(columns=internal_cols)


def esri_truncate_columns(columns: list | pd.Index) -> dict:
    """
    from a list of columns or df.columns, match gdal algorithm
    to generate ESRI Shapefile truncated names. Includes handling
    truncated duplicates.

    https://gdal.org/en/stable/drivers/vector/shapefile.html

    Intended for use after all other renaming complete.
    """
    truncated_cols = []
    for col in columns:
        if col[:10] not in truncated_cols:
            truncated_cols += [col[:10]]
        else:  # truncated duplicate present
            for i in range(1, 101):
                if i > 99:
                    raise Exception("gdal does not support more than 99 truncated duplicates")
                suffix = str(i).rjust(2, "_")  # pad single digits with _ on left
                if col[:8] + suffix not in truncated_cols:
                    truncated_cols += [col[:8] + suffix]
                    break
    truncated_dict = dict(zip(truncated_cols, columns))
    truncated_dict = {key: truncated_dict[key] for key in truncated_dict.keys() if key != truncated_dict[key]}
    return truncated_dict
