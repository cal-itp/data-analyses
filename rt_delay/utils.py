import shared_utils

import gcsfs
import shapely

import branca

GCS_PROJECT = "cal-itp-data-infra"
BUCKET_NAME = "calitp-analytics-data"
BUCKET_DIR = "data-analyses/rt_delay"
GCS_FILE_PATH = f"gs://{BUCKET_NAME}/{BUCKET_DIR}/"

MPH_PER_MPS = 2.237 ## use to convert meters/second to miles/hour

def reversed_colormap(existing):
    return branca.colormap.LinearColormap(
        colors=list(reversed(existing.colors)),
        vmin=existing.vmin, vmax=existing.vmax
    )

def primary_cardinal_direction(origin, destination):
    distance_east = destination.x - origin.x
    distance_north = destination.y - origin.y
    
    if abs(distance_east) > abs(distance_north):
        if distance_east > 0:
            return('Eastbound')
        else:
            return('Westbound')
    else:
        if distance_north > 0:
            return('Northbound')
        else:
            return('Southbound')