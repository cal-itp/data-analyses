"""
Google Directions API

Save request response (dict) as json to cache.
Each request (one trip / route) can take up to 25 waypoints 
before being broken up into 2 separate requests.

$10 for first 1,000 requests
$8 for next 1,000
"""
import dotenv
import googlemaps
import glob
import os
import pandas as pd
import sys

from loguru import logger

from bus_service_utils import utils
from D1_setup_parallel_trips_with_stops import ANALYSIS_DATE

logger.add("./logs/make_gmaps_requests.log")
logger.add(sys.stderr, 
           format="{time:YYYY-MM-DD at HH:mm:ss} | {level} | {message}", 
           level="INFO")

dotenv.load_dotenv("_env")

GOOGLE_API_KEY = os.environ["GOOGLE_API_KEY"]

DATA_PATH = "./gmaps_cache/"
GCS_FILE_PATH = f"{utils.GCS_FILE_PATH}gmaps_cache_{ANALYSIS_DATE}/"


if __name__ == "__main__":
    df = pd.read_parquet(f"{utils.GCS_FILE_PATH}gmaps_df_{ANALYSIS_DATE}.parquet")
    gmaps = googlemaps.Client(key=GOOGLE_API_KEY)

    # Check if there are any identifiers already cached
    # Drop those from our df
    FILES = [f for f in glob.glob(f"{DATA_PATH}*.json")]
    filenames = []
    for f in FILES:
        file = f.strip(f'{DATA_PATH}').strip('.json')
        filenames.append(file)

    logger.info(f"# rows: {len(df)}")
    df = df[~df.identifier.isin(filenames)]
    logger.info(f"# rows after local caches included: {len(df)}")

    origin = df.origin.tolist()
    destination = df.destination.tolist()
    departures = df.departure_in_one_year.tolist()
    waypoints = df.waypoints.tolist()
    identifiers = df.identifier_num.tolist()
    identifier_str = df.identifier.tolist()
    
    for i, (o, d) in enumerate(zip(origin, destination)):
        try:
            result = gmaps.directions(
                o, d, 
                mode='driving', 
                departure_time=departures[i], 
                waypoints=[f"via:{lat},{lon}" for lat, lon in waypoints[i]]
            )
            '''
            # Use via waypoints to get duration_in_traffic
            # longest travel time compared to sum(waypoints)
            via = gmaps.directions(origin, destination, 
                             mode='driving', 
                             departure_time=departures, 
                             waypoints=[f"via:{lat},{lon}" for lat, lon in waypoints]
                            )
            '''
            utils.save_request_json(
                result, identifiers[i], 
                # Using different paths than utils.DATA_PATH, utils.GCS_FILE_PATH
                data_path = DATA_PATH,
                gcs_file_path = GCS_FILE_PATH
            )
            logger.info(f"Saved {identifier_str[i]}")
            
        except:
            logger.info(f"No result: {identifiers[i]}")
