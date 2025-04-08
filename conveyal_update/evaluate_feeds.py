import os
os.environ["CALITP_BQ_MAX_BYTES"] = str(800_000_000_000)

from shared_utils import gtfs_utils_v2
from calitp_data_analysis.tables import tbls
from calitp_data_analysis.sql import query_sql
from siuba import *
import pandas as pd
import datetime as dt
import conveyal_vars

TARGET_DATE = conveyal_vars.TARGET_DATE
REGIONAL_SUBFEED_NAME = "Regional Subfeed"
INT_TO_GTFS_WEEKDAY = {
    0: "monday",
    1: "tuesday",
    2: "wednesday",
    3: "thursday",
    4: "friday",
    5: "saturday",
    6: "sunday"
}

def check_defined_elsewhere(row, df):
    '''
    for feeds without service defined, check if the same service is captured in another feed that does include service
    '''
    is_defined = ((df >> filter(-_.n.isna())).service_key == row.service_key).any()
    row['service_any_feed'] = is_defined
    return row

def get_feeds_check_service():
    feeds_on_target = gtfs_utils_v2.schedule_daily_feed_to_gtfs_dataset_name(selected_date=TARGET_DATE)
    feeds_on_target = feeds_on_target.rename(columns={'name':'gtfs_dataset_name'})
    # default will use mtc subfeeds (prev Conveyal behavior), can spec customer facing if we wanna switch

    operator_feeds = feeds_on_target.feed_key
    trips = (
        tbls.mart_gtfs.fct_scheduled_trips()
        >> filter(_.feed_key.isin(operator_feeds), _.service_date == TARGET_DATE)
        >> group_by(_.feed_key)
        >> count(_.feed_key)
        # >> collect()
        # >> mutate(any_trip = True)
    )
    service_defined = trips >> collect()
    feeds_on_target = feeds_on_target >> left_join(_, service_defined, on = 'feed_key') >> select(-_.name)
    return feeds_on_target
    
def attach_transit_services(feeds_on_target: pd.DataFrame):
    """Associate each feed in feeds_on_target.gtfs_dataset_key with a transit service"""
    target_dt = dt.datetime.combine(dt.date.fromisoformat(TARGET_DATE), dt.time(0))

    services = (tbls.mart_transit_database.dim_gtfs_service_data()
        >> filter(
            _._valid_from <= target_dt, _._valid_to > target_dt
        )
        # >> filter(_.gtfs_dataset_key == 'da7e9e09d3eec6c7686adc21c8b28b63') # test with BCT
        # >> filter(_.service_key == '5bc7371dca26d74a99be945b18b3174e')
        >> select(_.service_key, _.gtfs_dataset_key, _.customer_facing)
        >> collect()
    )

    feeds_services_merged = feeds_on_target.merge(
        services, how="left", on='gtfs_dataset_key', validate="one_to_many"
    )           
    feeds_services_filtered = feeds_services_merged.loc[
        feeds_services_merged["customer_facing"] | (feeds_services_merged["regional_feed_type"] == REGIONAL_SUBFEED_NAME)
    ].copy()
    return feeds_services_filtered
        
def get_undefined_feeds(feeds_on_target: pd.DataFrame) -> pd.DataFrame:
    """Return feeds in feeds_on_target that do not have service and where service is not defined in another feed"""
    undefined = feeds_on_target.apply(check_defined_elsewhere, axis=1, args=[feeds_on_target]) >> filter(-_.service_any_feed)
    return undefined

def report_unavailable_feeds(feeds: pd.DataFrame, fname: str) -> None:
    """Create a csv report of unavailable or backdated feeds at the paths specified in fname"""
    undefined = feeds.loc[
        feeds["valid_date_other_than_service_date"] | feeds["no_schedule_feed_found"]
    ].copy()
    if undefined.empty:
        print('no undefined service feeds')
    else:
        print('these feeds have no service defined on target date, nor are their services captured in other feeds:')
        print(undefined.loc[undefined["no_schedule_feed_found"], "gtfs_dataset_name"].drop_duplicates())
        print('these feeds have defined service, but only in a feed defined on a prior day')
        print(undefined.loc[undefined["valid_date_other_than_service_date"], "gtfs_dataset_name"].drop_duplicates())
        print(f'saving detailed csv to {fname}')
        undefined.to_csv(fname, index=False)

ISO_DATE_ONLY_FORMAT = "%y-%m-%d"

def get_old_feeds(undefined_feeds_base64_urls: pd.Series, target_date: dt.date | dt.datetime, max_lookback_timedelta: dt.timedelta) -> pd.Series:
    """
    Search the warehouse for feeds downloaded within the time before target_date 
    defined by max_lookback_timedelta that have service as defined in calendar.txt 
    on target_date. These feeds will not be valid on target_date, but will be accepted by Conveyal.
    This should not be used if the feeds are valid on the target_date, since this will provide needlessly
    invalid feeds. Note that this does not check calendar_dates.txt at present

    Parameters:
    undefined_feeds_base64_urls: a Pandas series containing base64 urls to feeds in the warehouse
    target_date: a date or datetime where the feeds should be valid based on calendar.txt
    max_lookback_timedelta: a timedelta defining the amount of time before target_date that a feed must have been available for
    
    Returns:
    A DataFrame with the following index and columns:
        index: The base64 url of the feed, will match entries in undefined_feeds_base64_urls
        feed_key: A key to dim_schedule_feeds matching the feed on the date it was last valid in the warehouse
        date_processed: A datetime date matching the date on which the feed was last valid in the warehosue
    """
    base_64_urls_str = "('" + "', '".join(undefined_feeds_base64_urls) + "')"
    day_of_the_week = INT_TO_GTFS_WEEKDAY[target_date.weekday()]
    max_lookback_date = target_date - max_lookback_timedelta
    target_date_iso = target_date.strftime(ISO_DATE_ONLY_FORMAT)
    # Query feeds for the newest feed where service is defined on the target_date,
    # that have service on the day of the week of the target date, and
    # that are valid before (inclusive) the target date and after (inclusive) the max look back date,
    query = f"""
    SELECT 
      `mart_gtfs.dim_schedule_feeds`.base64_url AS base64_url,
      `mart_gtfs.dim_schedule_feeds`.key as feed_key,
      MAX(`mart_gtfs.dim_schedule_feeds`._valid_to) AS valid_feed_date
    from `mart_gtfs.dim_schedule_feeds`
    LEFT JOIN `mart_gtfs.dim_calendar`
    ON `mart_gtfs.dim_schedule_feeds`.key = `mart_gtfs.dim_calendar`.feed_key
    WHERE `mart_gtfs.dim_schedule_feeds`.base64_url IN {base_64_urls_str}
      AND `mart_gtfs.dim_schedule_feeds`._valid_to >= '{max_lookback_date}'
      AND `mart_gtfs.dim_schedule_feeds`._valid_to <= '{target_date}'
      AND `mart_gtfs.dim_calendar`.{day_of_the_week} = 1
      AND `mart_gtfs.dim_calendar`.start_date <= '{target_date}'
      AND `mart_gtfs.dim_calendar`.end_date >= '{target_date}'
    GROUP BY 
        `mart_gtfs.dim_schedule_feeds`.base64_url, 
        `mart_gtfs.dim_schedule_feeds`.key
    LIMIT 1000
    """
    response = query_sql(
        query
    )
    response_grouped = response.groupby("base64_url")
    feed_info_by_url = response_grouped[["valid_feed_date", "feed_key"]].first()
    feed_info_by_url["date_processed"] = feed_info_by_url["valid_feed_date"].dt.date - dt.timedelta(days=1) 
    # we have the day the feed becomes invalid, so the day we are interested in where the feed *is* valid is the day after
    return feed_info_by_url.drop("valid_feed_date", axis=1)

def merge_old_feeds(df_all_feeds: pd.DataFrame, df_undefined_feeds: pd.DataFrame, target_date: dt.date, max_lookback_timedelta: dt.timedelta) -> pd.DataFrame:
    """
    Merge feeds from df_all_feeds with old feeds found as a result of calling get_old_feeds with df_undefined_feeds.base64_url
    
    Params:
    df_all_feeds: A DataFrame of feeds, must have feed_key, date, and base64_url as columns and must include the base64_urls in df_undefined_feeds
    df_undefined_feeds: A DataFrame of feeds that are not valid on target_date, where an old feed should be searched for.
        Must have base64_url as a column
    target_date: a date or datetime where the feed should be valid based on its target date
    max_lookback_timedelta: a timedelta defining the amount of time before target_date that a feed must have been available for   
    
    Returns:
    A DataFrame identical to df_all_feeds except with the following columns changed or added:
        feed_key: Updated for the found feeds
        date: Updated for the found feeds:
        no_schedule_feed_found: True if a schedule feed was present in df_undefined_feeds but was not associated with an older feed, otherwise false 
        valid_date_other_than_service_date: True if a new feed was found, otherwise false
    """
    feed_search_result = get_old_feeds(
        df_undefined_feeds["base64_url"], 
        target_date,
        max_lookback_timedelta
    )
    feeds_merged = df_all_feeds.merge(
        feed_search_result,
        how="left",
        left_on="base64_url",
        right_index=True,
        validate="many_to_one"
    ) 
    feeds_merged["feed_key"] = feeds_merged["feed_key_y"].fillna(feeds_merged["feed_key_x"])
    feeds_merged["no_schedule_feed_found"] = (
        (feeds_merged["base64_url"].isin(df_undefined_feeds["base64_url"])) & (~feeds_merged["base64_url"].isin(feed_search_result.index))
    ).fillna(False)
    feeds_merged["date"] = feeds_merged["date_processed"].fillna(target_date)
    feeds_merged["valid_date_other_than_service_date"] = feeds_merged["date"] != target_date

    return feeds_merged.drop(
        ["date_processed", "feed_key_x", "feed_key_y"], axis=1
    )

if __name__ == '__main__':
    
    feeds_on_target = get_feeds_check_service()
    feeds_on_target = attach_transit_services(feeds_on_target)
    print(f'feeds on target date shape: {feeds_on_target.shape}')
    undefined_feeds = get_undefined_feeds(feeds_on_target)
    feeds_merged = merge_old_feeds(
        feeds_on_target, undefined_feeds, dt.date.fromisoformat(TARGET_DATE), conveyal_vars.LOOKBACK_TIME
    )
    report_unavailable_feeds(feeds_merged, 'no_apparent_service.csv')
    feeds_merged.to_parquet(f'{conveyal_vars.GCS_PATH}feeds_{TARGET_DATE}.parquet')
    