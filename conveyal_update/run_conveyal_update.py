from raw_feed_download_utils.evaluate_feeds import *
import conveyal_vars

TARGET_DATE = "2025-06-11"

if __name__ == "__main__":
    # copied from evaluate_feeds.py
    feeds_on_target = get_feeds_check_service(TARGET_DATE)
    feeds_on_target = attach_transit_services(feeds_on_target, TARGET_DATE)
    print(f'feeds on target date shape: {feeds_on_target.shape}')
    undefined_feeds = get_undefined_feeds(feeds_on_target)
    feeds_merged = merge_old_feeds(
        feeds_on_target, undefined_feeds, dt.date.fromisoformat(TARGET_DATE), conveyal_vars.LOOKBACK_TIME
    )
    report_unavailable_feeds(feeds_merged, 'no_apparent_service.csv')
    feeds_merged.to_parquet(f'{conveyal_vars.GCS_PATH}feeds_{TARGET_DATE}.parquet')

    # copied from 