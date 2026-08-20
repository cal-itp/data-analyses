import google.auth
import numpy as np
import pandas as pd
import pygris 
import geopandas as gpd
from calitp_data_analysis import geography_utils
from calitp_data_analysis.sql import to_snakecase
from shared_utils import arcgis_query

from calitp_data_analysis import get_fs
fs = get_fs()

@cache
def gcs_geopandas():
    return GCSGeoPandas()

import google.auth
import pandas_gbq

credentials, project = google.auth.default()
from functools import cache

from calitp_data_analysis.gcs_pandas import GCSPandas
from calitp_data_analysis.gcs_geopandas import GCSGeoPandas

def _parse_gcs_path(gcs_path: str):
    """
    Split a GCS URL into (bucket, prefix) without leading 'gs://'.
    """
    if not gcs_path.startswith("gs://"):
        raise ValueError(f"Expected a 'gs://' path, got: {gcs_path}")
    no_scheme = gcs_path[5:]
    bucket, *rest = no_scheme.split("/", 1)
    prefix = rest[0] if rest else ""
    if prefix and not prefix.endswith("/"):
        prefix += "/"
    return bucket, prefix

def list_gcs_files(gcs_folder: str, extensions: Optional[List[str]] = None) -> list:
    """
    List all files in a GCS 'folder' (prefix). Optionally filter by extensions.
    Returns full 'gs://...' URIs.
    """
    bucket_name, prefix = _parse_gcs_path(gcs_folder)
    client = storage.Client()
    bucket = client.bucket(bucket_name)

    uris: List[str] = []
    for blob in client.list_blobs(bucket_name, prefix=prefix):
        # Skip "directory placeholders"
        name = blob.name
        if name.endswith("/"):
            continue
        if extensions:
            if not any(name.lower().endswith(ext.lower()) for ext in extensions):
                continue
        uris.append(f"gs://{bucket_name}/{name}")

    return sorted(uris)

def concat_gcs_folder(
    gcs_folder: str = "gs://calitp-analytics-data/data-analyses/equity_index/tims/",
    prefer_arrow_dataset: bool = True,
    file_types: Optional[List[str]] = None,
    geometry: bool = False,
    dtype_overrides: Optional[dict] = None,
    use_threads: bool = True,
) -> Union[pd.DataFrame, "gpd.GeoDataFrame"]:
    """
    Concatenate all files in a GCS folder into a single DataFrame.
    """

    # Default supported formats
    if file_types is None:
        file_types = ["parquet", "csv", "feather", "geojson", "json"]

    files = list_gcs_files(gcs_folder, extensions=[f".{ext}" for ext in file_types])

    if not files:
        raise FileNotFoundError(f"No files found under: {gcs_folder} with types {file_types}")

    # Disable arrow.dataset for parquet — because GCS credentials fail there
    all_parquet = all(f.lower().endswith(".parquet") for f in files)
    
    # Updated behavior: Always use gcs_geopandas().read_parquet for parquet files
    if all_parquet:
        frames = []
        for uri in files:
            frames.append(gcs_geopandas().read_parquet(uri))
        df = pd.concat(frames, ignore_index=True)

        if dtype_overrides:
            df = df.astype(dtype_overrides, errors="ignore")
        return df

    # Otherwise fall back to file-by-file reading
    frames: List[Union[pd.DataFrame, "gpd.GeoDataFrame"]] = []

    for uri in files:
        lower = uri.lower()

        if lower.endswith(".parquet"):
            frames.append(gcs_geopandas().read_parquet(uri))

        elif lower.endswith(".feather"):
            frames.append(pd.read_feather(uri))

        elif lower.endswith(".csv"):
            frames.append(pd.read_csv(uri, low_memory=False))

        elif lower.endswith(".geojson") or (lower.endswith(".json") and "geo" in os.path.basename(uri).lower()):
            if not _HAS_GPD:
                raise ImportError("geopandas not installed—install it or set geometry=False.")
            gdf = gpd.read_file(uri)
            frames.append(gdf)

        else:
            print(f"[concat_gcs_folder] Skipping unsupported file: {uri}")

    if not frames:
        raise FileNotFoundError(f"Found files, but none were readable with the allowed types: {file_types}")

    # If any frames are GeoDataFrames, concatenate as GeoDataFrame
    if _HAS_GPD and any(isinstance(f, gpd.GeoDataFrame) for f in frames):
        df = pd.concat(frames, ignore_index=True)
        if "geometry" in df.columns and not isinstance(df, gpd.GeoDataFrame):
            df = gpd.GeoDataFrame(df, geometry="geometry", crs=frames[0].crs if hasattr(frames[0], "crs") else None)
    else:
        df = pd.concat(frames, ignore_index=True)

    if dtype_overrides:
        df = df.astype(dtype_overrides, errors="ignore")

    df = to_snakecase(df)
    return df
