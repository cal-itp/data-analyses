"""
Functions related to maps.
"""
import pandas as pd


# Centroids for various regions and zoom level
def grab_region_centroids() -> dict:
    # This parquet is created in shared_utils/shared_data.py
    df = pd.read_parquet("gs://calitp-analytics-data/data-analyses/shared_data/ca_county_centroids.parquet")

    df = df.assign(centroid=df.centroid.apply(lambda x: x.tolist()))

    # Manipulate parquet file to be dictionary to use in map_utils
    region_centroids = dict(zip(df.county_name, df[["centroid", "zoom"]].to_dict(orient="records")))

    return region_centroids


REGION_CENTROIDS = grab_region_centroids()
