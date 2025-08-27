import os
from calitp_data_analysis import get_fs
import traceback
import pandas as pd

from tqdm import tqdm
tqdm.pandas()

fs = get_fs()

import shutil

from .entities import BoundingBoxDict

def download_feed(row):
    # need wildcard for file too -- not all are gtfs.zip!
    try:
        uri = f'gs://calitp-gtfs-schedule-raw-v2/schedule/dt={row.date.strftime("%Y-%m-%d")}/*/base64_url={row.base64_url}/*.zip'
        # Get the exact path on GCS by globbing
        glob_result = tuple(fs.glob(uri))
        if len(glob_result) == 0:
            print(f"File not found at {uri}")
            return
        elif len(glob_result) > 1:
            print(f"More than one file found at {uri}")
            return
        # Download the zip file if there is only one possoble option
        fs.get_file(
            glob_result[0],
            os.path.join(row.path, f'{row.gtfs_dataset_name.replace(" ", "_")}_{row.feed_key}_gtfs.zip')
        )
    except Exception as e:
        print("Could not download feed. Traceback below:")
        print(traceback.format_exception(e))

def download_feeds(feeds_df: pd.DataFrame, output_path: os.PathLike):
    if not os.path.exists(output_path):
        os.mkdir(output_path)
    path_series = pd.Series(output_path, index=feeds_df.index, name="path")
    feeds_with_path = pd.concat([feeds_df, path_series], axis=1)
    feeds_with_path.apply(download_feed, axis=1)  

def download_region(feeds_df, region_name: str):
    path = f'./feeds_{feeds_df.date.iloc[0].strftime("%Y-%m-%d")}/{region_name}'
    if not os.path.exists(path): 
        os.makedirs(path)
    region_name = feeds_df.loc[feeds_df.region == region_name].copy()
    region_name['path'] = path
    region_name.progress_apply(download_feed, axis = 1)
    
def generate_script(regions: BoundingBoxDict):
    #  https://docs.conveyal.com/prepare-inputs#preparing-the-osm-data
    cmds = []
    for region in regions.keys():
        cmd = f'''osmosis --read-pbf us-west-latest.osm.pbf --bounding-box left={regions[region]['west']} bottom={regions[region]['south']} right={regions[region]['east']} top={regions[region]['north']} --tf accept-ways highway=* public_transport=platform railway=platform park_ride=* --tf accept-relations type=restriction --used-node --write-pbf {region}-processed.pbf'''
        cmds += [cmd]
    with open('crop_filter_osm.sh', "w") as f:
        f.write('#!/bin/bash\n')
        f.write('wget http://download.geofabrik.de/north-america/us-west-latest.osm.pbf\n')
        f.write('\n'.join(cmds))      
        
if __name__ == '__main__':
    regions_and_feeds = pd.read_parquet(f'{conveyal_vars.GCS_PATH}regions_feeds_{TARGET_DATE}.parquet')
    
    for region in tqdm(regions.keys()):
        download_region(regions_and_feeds, region)
    shutil.make_archive(f'feeds_{TARGET_DATE}', 'zip', f'./feeds_{TARGET_DATE}/')
    fs.put(f'feeds_{TARGET_DATE}.zip', f'{conveyal_vars.GCS_PATH}feeds_{TARGET_DATE}.zip')
    generate_script(regions)