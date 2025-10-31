"""
Functions to bridge GTFS schedule and RT.
"""
from typing import Literal, Union

import dask.dataframe as dd
import dask_geopandas as dg
import geopandas as gpd
import pandas as pd
from calitp_data_analysis.sql import get_engine, query_sql

PACIFIC_TIMEZONE = "US/Pacific"

def _query_sql_with_params(query_template: str, search_criteria: dict, as_df: bool) -> pd.DataFrame:
    # TODO: update query_sql to accept parameterized queries and use that instead
    search_conditions = ""
    search_params = {}

    for k, v in (search_criteria or {}).items():
        search_conditions = f" AND {k} IN UNNEST(%({k}_values)s)"
        search_params[f"{k}_values"] = v

    query = query_template.format(search_conditions=search_conditions)
    db_engine = get_engine()

    with db_engine.connect() as connection:
        if as_df:
            result = pd.read_sql(query, connection, params=search_params)
        else:
            result = connection.execute(query, params=search_params)

    return result

def localize_timestamp_col(df: dd.DataFrame, timestamp_col: Union[str, list]) -> dd.DataFrame:
    """
    RT vehicle timestamps are given in UTC.
    Localize these to Pacific Time.
    """
    # https://stackoverflow.com/questions/62992863/trying-to-convert-aware-local-datetime-to-naive-local-datetime-in-panda-datafram

    if isinstance(timestamp_col, str):
        timestamp_col = [timestamp_col]

    for c in timestamp_col:
        if isinstance(df, (dd.DataFrame, dg.GeoDataFrame)):
            localized_timestamp_col = (
                dd.to_datetime(df[c], utc=True)
                .dt.tz_convert(PACIFIC_TIMEZONE)
                .apply(lambda t: t.replace(tzinfo=None), meta=(None, "datetime64[ns]"))
            )
        elif isinstance(df, (pd.DataFrame, gpd.GeoDataFrame)):
            localized_timestamp_col = (
                pd.to_datetime(df[c], utc=True).dt.tz_convert(PACIFIC_TIMEZONE).apply(lambda t: t.replace(tzinfo=None))
            )

        df[f"{c}_local"] = localized_timestamp_col

    return df


def get_schedule_gtfs_dataset_key(date: str, get_df: bool = True, **kwargs) -> pd.DataFrame:
    """
    Use fct_daily_feed_scheduled_service to find
    the gtfs_dataset_key that corresponds to the feed_key.
    """
    project = kwargs.get("project", "cal-itp-data-infra")
    dataset = kwargs.get("dataset", "mart_gtfs")

    return query_sql(f"""
        SELECT gtfs_dataset_key, feed_key FROM {project}.{dataset}.fct_daily_feed_scheduled_service_summary
        WHERE service_date = '{date}'
    """, as_df=get_df)


def filter_dim_gtfs_datasets(
    keep_cols: list[str] = ["key", "name", "type", "regional_feed_type", "uri", "base64_url"],
    custom_filtering: dict = None,
    get_df: bool = True,
    **kwargs
) -> pd.DataFrame:
    """
    Filter mart_transit_database.dim_gtfs_dataset table
    and keep only the valid rows that passed data quality checks.
    """
    project = kwargs.get("project", "cal-itp-data-infra")
    dataset = kwargs.get("dataset", "mart_transit_database")

    if "key" not in keep_cols:
        raise KeyError("Include key in keep_cols list")

    columns = []

    for column in keep_cols:
        new_column = column

        if column in ["key", "name"]:
            new_column = f"{column} AS gtfs_dataset_{column}"

        columns.append(new_column)

    query_base = f"""
        SELECT {','.join(columns)}
        FROM {project}.{dataset}.dim_gtfs_datasets
        WHERE data_quality_pipeline = true
    """

    query_template = query_base + "{search_conditions}"

    return _query_sql_with_params(query_template=query_template, search_criteria=custom_filtering, as_df=get_df)


def get_organization_id(
    df: pd.DataFrame,
    date: str,
    merge_cols: list = [],
    **kwargs,
) -> pd.DataFrame:
    """
    Instead of using the GTFS dataset name (of the quartet), usually
    LA Metro Schedule, LA Metro Trip Updates, etc, always
    publish with the organization name, LA Metro.

    Input a date to filter down what feeds were valid.
    Merge columns must be defined and hold prefixes indicating the quartet.
    Ex: schedule_gtfs_dataset_key, vehicle_positions_gtfs_dataset_name
    """
    quartet = ["schedule", "vehicle_positions", "service_alerts", "trip_updates"]

    datasets = ["gtfs_dataset_key", "gtfs_dataset_name", "source_record_id"]

    # https://stackoverflow.com/questions/2541401/pairwise-crossproduct-in-python
    quartet_cols = [f"{q}_{d}" for q in quartet for d in datasets]

    # All the merge cols must be found in quartet_cols
    # This is flexible enough so we can take just gtfs_dataset_key or name to merge too
    if not all(c in quartet_cols for c in merge_cols):
        raise KeyError(
            "Unable to detect which GTFS quartet "
            f"these columns {df.columns}. "
            "Rename to [quartet]_gtfs_dataset_key, "
            "[quartet]_gtfs_dataset_name. "
            "Valid quartet values: schedule, vehicle_positions, "
            "trip_updates, or service_alerts."
        )
    else:
        project = kwargs.get("project", "cal-itp-data-infra")
        dataset = kwargs.get("dataset", "mart_transit_database")

        dim_provider_gtfs_data = query_sql(f"""
            SELECT DISTINCT *
            FROM {project}.{dataset}.dim_provider_gtfs_data
            WHERE DATETIME(_valid_from, '{PACIFIC_TIMEZONE}') <= DATETIME('{date}')
                AND DATETIME(_valid_to, '{PACIFIC_TIMEZONE}') >= DATETIME('{date}')
        """, as_df=True)

        sorting = [True for c in merge_cols]
        keep_cols = ["organization_source_record_id"]

        # We allow fanout when merging a feed to multiple organization names,
        # but we should handle it by selectig a preferred
        # rather than alphabetical.
        # (organization names Foothill Transit and City of Duarte)
        dim_provider_gtfs_data = dim_provider_gtfs_data.sort_values(
            merge_cols + ["_valid_to", "_valid_from"], ascending=sorting + [False, False]
        ).reset_index(drop=True)[merge_cols + keep_cols]

        df2 = pd.merge(df, dim_provider_gtfs_data, on=merge_cols, how="inner")

        return df2


def filter_dim_county_geography(
    date: str,
    keep_cols: list[str] = [],
    **kwargs,
) -> pd.DataFrame:
    """
    Merge mart_transit_database.dim_county_geography with
    mart_transit_database.bridge_organizations_x_headquarters_county_geography.
    Both tables are at organization-county-feed_period grain.

    dim_county_geography holds additional geography columns like
    MSA, FIPS, etc.

    Use this merge to get caltrans_district.
    Organizations belong to county, and counties are assigned to districts.
    """
    project = kwargs.get("project", "cal-itp-data-infra")
    dataset = kwargs.get("dataset", "mart_transit_database")

    df = query_sql(f"""
        SELECT 
            bohcg.organization_name,
            CONCAT(LPAD(CAST(dmg.caltrans_district AS STRING), 2, '0'), ' - ', dmg.caltrans_district_name) AS caltrans_district,
            {','.join(keep_cols)}
        FROM {project}.{dataset}.bridge_organizations_x_headquarters_county_geography AS bohcg
        INNER JOIN {project}.{dataset}.dim_county_geography AS dmg ON dmg.key = bohcg.county_geography_key
        WHERE DATETIME(bohcg._valid_from, '{PACIFIC_TIMEZONE}') <= DATETIME('{date}')
            AND DATETIME(bohcg._valid_to, '{PACIFIC_TIMEZONE}') >= DATETIME('{date}')
    """, as_df=True)

    return df[["organization_name", "caltrans_district"] + keep_cols].drop_duplicates().reset_index(drop=True)


def filter_dim_organizations(
    keep_cols: list[str] = ["source_record_id"],
    custom_filtering: dict = None,
    get_df: bool = True,
    **kwargs,
) -> pd.DataFrame:
    """
    Filter dim_organizations down to current record for organization.
    Caltrans district is associated with organization_source_record_id.
    """
    project = kwargs.get("project", "cal-itp-data-infra")
    dataset = kwargs.get("dataset", "mart_transit_database")
    columns = []

    for column in keep_cols:
        if column == "source_record_id":
            columns.append("source_record_id AS organization_source_record_id")
        else:
            columns.append(column)

    query_base = f"SELECT {','.join(columns)} FROM {project}.{dataset}.dim_organizations WHERE _is_current = true"
    query_template = query_base + "{search_conditions}"

    return _query_sql_with_params(query_template=query_template, search_criteria=custom_filtering, as_df=get_df)


def sample_gtfs_dataset_key_to_organization_crosswalk(
    df: pd.DataFrame,
    date: str,
    quartet_data: Literal["schedule", "vehicle_positions", "service_alerts", "trip_updates"],
    dim_gtfs_dataset_cols: list[str] = [
        "key",
        "name",
        "type",
        "source_record_id",
        "regional_feed_type",
        "base64_url",
        "uri",
    ],
    dim_organization_cols: list[str] = ["source_record_id", "name"],
    dim_county_geography_cols: list[str] = ["caltrans_district"],
    **kwargs,
) -> pd.DataFrame:
    """
    Get crosswalk from gtfs_dataset_key to certain quartet data identifiers
    like base64_url, uri, and organization identifiers
    like organization_source_record_id and caltrans_district.
    """
    project = kwargs.get("project", "cal-itp-data-infra")
    dataset = kwargs.get("dataset", "mart_transit_database")

    id_cols = ["gtfs_dataset_key"]

    # If schedule feed_key is present, include it our crosswalk output
    if "feed_key" in df.columns:
        id_cols = ["feed_key"] + id_cols

    feeds = df[id_cols].drop_duplicates().reset_index(drop=True)

    # (1) Filter dim_gtfs_datasets by quartet and merge with the
    # gtfs_dataset_keys in df.
    dim_gtfs_datasets = filter_dim_gtfs_datasets(
        keep_cols=dim_gtfs_dataset_cols,
        custom_filtering={"type": [quartet_data]},
        get_df=True,
        project=project,
        dataset=dataset,
    )

    feeds_with_quartet_info = pd.merge(
        feeds,
        dim_gtfs_datasets,
        on="gtfs_dataset_key",
        how="inner",
    )

    # (2) Rename key/name to reflect the quartet
    GTFS_DATASET_RENAME_DICT = {
        "gtfs_dataset_key": f"{quartet_data}_gtfs_dataset_key",
        "gtfs_dataset_name": f"{quartet_data}_gtfs_dataset_name",
        "source_record_id": f"{quartet_data}_source_record_id",
    }

    feeds_with_quartet_info = feeds_with_quartet_info.rename(columns=GTFS_DATASET_RENAME_DICT)

    # (3) From quartet, get to organization name
    merge_cols = [i for i in feeds_with_quartet_info.columns if quartet_data in i]

    feeds_with_org_id = get_organization_id(
        feeds_with_quartet_info,
        date,
        merge_cols=merge_cols,
        project=project,
        dataset=dataset,
    )

    # (4) Merge in dim_orgs to get organization info - everything except caltrans_district is found here
    ORG_RENAME_DICT = {"source_record_id": "organization_source_record_id", "name": "organization_name"}
    orgs = filter_dim_organizations(
        keep_cols=dim_organization_cols,
        get_df=True,
        project=project,
        dataset=dataset,
    ).rename(columns=ORG_RENAME_DICT)

    feeds_with_org_info = pd.merge(feeds_with_org_id, orgs, on="organization_source_record_id")

    # (5) Merge in dim_county_geography to get caltrans_district
    # https://github.com/cal-itp/data-analyses/issues/1282
    district = filter_dim_county_geography(date, dim_county_geography_cols, project=project, dataset=dataset)

    feeds_with_district = pd.merge(feeds_with_org_info, district, on="organization_name")

    return feeds_with_district

