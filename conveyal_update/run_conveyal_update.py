#from raw_feed_download_utils.evaluate_feeds import *
from raw_feed_download_utils.match_feeds_regions import create_region_gdf
from raw_feed_download_utils.download_data import *
from raw_feed_download_utils.get_feeds import generate_feed_info
import datetime as dt
import conveyal_vars

TARGET_DATE = "2025-06-11"

if __name__ == "__main__":
    region_gdf = create_region_gdf(conveyal_vars.conveyal_regions)
    # copied from evaluate_feeds.py
    regions_and_feeds_merged = generate_feed_info(
        target_date=TARGET_DATE,
        lookback_period=dt.datetime(days=60),
        filter_geometry=region_gdf,
        filter_geometry_id="region",
    )
    """feeds_on_target = get_feeds_check_service(TARGET_DATE)
    feeds_on_target = attach_transit_services(feeds_on_target, TARGET_DATE)
    print(f'feeds on target date shape: {feeds_on_target.shape}')
    undefined_feeds = get_undefined_feeds(feeds_on_target)
    feeds_merged = merge_old_feeds(
        feeds_on_target, undefined_feeds, dt.date.fromisoformat(TARGET_DATE), conveyal_vars.LOOKBACK_TIME
    )
    report_unavailable_feeds(feeds_merged, 'no_apparent_service.csv')
    feeds_merged.to_parquet(f'{conveyal_vars.GCS_PATH}feeds_{TARGET_DATE}.parquet')

    # copied from match_feeds_regions.py
    regions_and_feeds = join_stops_regions(region_gdf, feeds_merged)
    regions_and_feeds_merged = regions_and_feeds.merge(
        feeds_merged[["feed_key", "gtfs_dataset_name", "base64_url", "date"]],
        how="inner",
        on="feed_key",
    )"""
    regions_and_feeds_merged.to_parquet(f'{conveyal_vars.GCS_PATH}regions_feeds_{TARGET_DATE}.parquet')

    # copied from download_utils.py
    for region in tqdm(conveyal_vars.conveyal_regions.keys()):
        download_region(regions_and_feeds_merged, region)
    shutil.make_archive(f'feeds_{TARGET_DATE}', 'zip', f'./feeds_{TARGET_DATE}/')
    fs.put(f'feeds_{TARGET_DATE}.zip', f'{conveyal_vars.GCS_PATH}feeds_{TARGET_DATE}.zip')
    generate_script(conveyal_vars.conveyal_regions)