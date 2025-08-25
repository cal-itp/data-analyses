#from raw_feed_download_utils.evaluate_feeds import *
from raw_feed_download_utils.match_feeds_regions import create_region_gdf
from raw_feed_download_utils.download_data import *
from raw_feed_download_utils.get_feed_info import get_feed_info
import datetime as dt
import conveyal_vars

TARGET_DATE = conveyal_vars.TARGET_DATE

if __name__ == "__main__":
    region_gdf = create_region_gdf(conveyal_vars.conveyal_regions)
    regions_and_feeds_merged = get_feed_info(
        target_date=TARGET_DATE,
        lookback_period=dt.timedelta(days=60),
        filter_geometry=region_gdf,
        filter_geometry_id="region",
        report_unavailable=True
    )
    # copied from old download_utils.py
    for region in tqdm(conveyal_vars.conveyal_regions.keys()):
        download_region(regions_and_feeds_merged, region)
    shutil.make_archive(f'feeds_{TARGET_DATE}', 'zip', f'./feeds_{TARGET_DATE}/')
    fs.put(f'feeds_{TARGET_DATE}.zip', f'{conveyal_vars.GCS_PATH}feeds_{TARGET_DATE}.zip')
    generate_script(conveyal_vars.conveyal_regions)