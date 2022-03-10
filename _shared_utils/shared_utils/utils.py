import base64
import os
import shutil

import fsspec
import geopandas as gpd
import pandas as pd
import requests
from calitp.storage import get_fs

fs = get_fs()


def import_csv_export_parquet(DATASET_NAME, OUTPUT_FILE_NAME, GCS_FILE_PATH, GCS=True):
    """
    DATASET_NAME: str. Name of csv dataset.
    OUTPUT_FILE_NAME: str. Name of output parquet dataset.
    GCS_FILE_PATH: str. Ex: gs://calitp-analytics-data/data-analyses/my-folder/

    """
    df = pd.read_csv(f"{DATASET_NAME}.csv")

    if GCS is True:
        df.to_parquet(f"{GCS_FILE_PATH}{OUTPUT_FILE_NAME}.parquet")
    else:
        df.to_parquet(f"./{OUTPUT_FILE_NAME}.parquet")


def geoparquet_gcs_export(gdf, GCS_FILE_PATH, FILE_NAME):
    """
    Save geodataframe as parquet locally,
    then move to GCS bucket and delete local file.

    gdf: geopandas.GeoDataFrame
    GCS_FILE_PATH: str. Ex: gs://calitp-analytics-data/data-analyses/my-folder/
    FILE_NAME: str. Filename.
    """
    gdf.to_parquet(f"./{FILE_NAME}.parquet")
    fs.put(f"./{FILE_NAME}.parquet", f"{GCS_FILE_PATH}{FILE_NAME}.parquet")
    os.remove(f"./{FILE_NAME}.parquet")


def download_geoparquet(GCS_FILE_PATH, FILE_NAME, save_locally=False):
    """
    Parameters:
    GCS_FILE_PATH: str. Ex: gs://calitp-analytics-data/data-analyses/my-folder/
    FILE_NAME: str, name of file (without the .parquet).
                Ex: test_file (not test_file.parquet)
    save_locally: bool, defaults to False. if True, will save geoparquet locally.
    """
    object_path = fs.open(f"{GCS_FILE_PATH}{FILE_NAME}.parquet")
    gdf = gpd.read_parquet(object_path)

    if save_locally is True:
        gdf.to_parquet(f"./{FILE_NAME}.parquet")

    return gdf


# Make zipped shapefile
# https://github.com/CityOfLosAngeles/planning-entitlements/blob/master/notebooks/utils.py
def make_zipped_shapefile(df, path):
    """
    Make a zipped shapefile and save locally
    Parameters
    ==========
    df: gpd.GeoDataFrame to be saved as zipped shapefile
    path: str, local path to where the zipped shapefile is saved.
            Ex: "folder_name/census_tracts"
                "folder_name/census_tracts.zip"

    Remember: ESRI only takes 10 character column names!!
    """
    # Grab first element of path (can input filename.zip or filename)
    dirname = os.path.splitext(path)[0]
    print(f"Path name: {path}")
    print(f"Dirname (1st element of path): {dirname}")
    # Make sure there's no folder with the same name
    shutil.rmtree(dirname, ignore_errors=True)
    # Make folder
    os.mkdir(dirname)
    shapefile_name = f"{os.path.basename(dirname)}.shp"
    print(f"Shapefile name: {shapefile_name}")
    # Export shapefile into its own folder with the same name
    df.to_file(driver="ESRI Shapefile", filename=f"{dirname}/{shapefile_name}")
    print(f"Shapefile component parts folder: {dirname}/{shapefile_name}")
    # Zip it up
    shutil.make_archive(dirname, "zip", dirname)
    # Remove the unzipped folder
    shutil.rmtree(dirname, ignore_errors=True)


# Function to overwrite file in GitHub
# Based on https://github.com/CityOfLosAngeles/aqueduct/tree/master/civis-aqueduct-utils/civis_aqueduct_utils

DEFAULT_COMMITTER = {
    "name": "Service User",
    "email": "my-email@email.com",
}


def upload_file_to_github(
    token,
    repo,
    branch,
    path,
    local_file_path,
    commit_message,
    committer=DEFAULT_COMMITTER,
):
    """
    Parameters
    ----------
    token: str
        GitHub personal access token and corresponds to GITHUB_TOKEN
        in Civis credentials.
    repo: str
        Repo name, such as 'CityofLosAngeles/covid19-indicators`
    branch: str
        Branch name, such as 'master'
    path: str
        Path to the file within the repo.
    local_file_path: str
        Path to the local file to be uploaded to the repo, which can differ
        from the path within the GitHub repo.
    commit_message: str
        Commit message used when making the git commit.
    commiter: dict
        name and email associated with the committer.
    """

    BASE = "https://api.github.com"

    # Get the sha of the previous version.
    # Operate on the dirname rather than the path itself so we
    # don't run into file size limitations.
    r = requests.get(
        f"{BASE}/repos/{repo}/contents/{os.path.dirname(path)}",
        params={"ref": branch},
        headers={"Authorization": f"token {token}"},
    )
    r.raise_for_status()
    item = next(i for i in r.json() if i["path"] == path)
    sha = item["sha"]

    # Upload the new version
    with fsspec.open(local_file_path, "rb") as f:
        contents = f.read()

    r = requests.put(
        f"{BASE}/repos/{repo}/contents/{path}",
        headers={"Authorization": f"token {token}"},
        json={
            "message": commit_message,
            "committer": committer,
            "branch": branch,
            "sha": sha,
            "content": base64.b64encode(contents).decode("utf-8"),
        },
    )
    r.raise_for_status()
