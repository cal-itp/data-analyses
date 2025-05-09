import datetime as dt
from shared_utils import rt_dates

GCS_PATH = 'gs://calitp-analytics-data/data-analyses/conveyal_update/'
TARGET_DATE = rt_dates.DATES['apr2025']  
LOOKBACK_TIME = dt.timedelta(days=60)
OSM_FILE = 'us-west-latest.osm.pbf'
PUBLISHED_FEEDS_YML_PATH = "../gtfs_funnel/published_operators.yml"
#  http://download.geofabrik.de/north-america/us-west-latest.osm.pbf
#  first download with wget...

conveyal_regions = {}
# boundaries correspond to Conveyal Analysis regions
conveyal_regions['norcal'] = {'north': 42.03909, 'south': 39.07038, 'east': -119.60541, 'west': -124.49158}
conveyal_regions['central'] = {'north': 39.64165, 'south': 35.87347, 'east': -117.53174, 'west': -123.83789}
conveyal_regions['socal'] = {'north': 35.8935, 'south': 32.5005, 'east': -114.13121, 'west': -121.46759}
conveyal_regions['mojave'] = {'north': 37.81629, 'south': 34.89945, 'east': -114.59015, 'west': -118.38043}
#  special region for one-off SR99 CMCP
# conveyal_regions['sr99'] = {'north': 38.71337, 'south': 34.81154, 'east': -118.66882, 'west': -121.66259}

# #  special region for one-off Centennial Corridor
# conveyal_regions['bakersfield'] = {'north': 36.81, 'south': 34.13, 'east': -117.12, 'west': -120.65}