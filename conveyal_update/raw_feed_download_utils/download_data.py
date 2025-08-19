import os
from calitp_data_analysis import get_fs
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
        fs.get(uri, f'{row.path}/{row.gtfs_dataset_name.replace(" ", "_")}_{row.feed_key}_gtfs.zip')
        # print(f'downloaded {row.path}/{row.feed_key}_gtfs.zip')
    except Exception as e:
        print(f'\n could not download feed at {e}')
    
def download_region(feeds_df, region_name: str):
    
    # assert region in regions.keys()  #TODO: replace with a string enum instead
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