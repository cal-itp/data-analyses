import base64
import fsspec
import os
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
    '''
    Save geodataframe as parquet locally, 
    then move to GCS bucket and delete local file.
    
    gdf: geopandas.GeoDataFrame
    GCS_FILE_PATH: str. Ex: gs://calitp-analytics-data/data-analyses/my-folder/
    FILE_NAME: str. Filename.
    '''
    gdf.to_parquet(f"./{FILE_NAME}.parquet")
    fs.put(f"./{FILE_NAME}.parquet", f"{GCS_FILE_PATH}{FILE_NAME}.parquet")
    os.remove(f"./{FILE_NAME}.parquet")


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