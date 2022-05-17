"""
Script to publish single notebooks.
Run this in data-analyses/bus_service_increase directory.
"""
import dotenv
import os
import subprocess
import sys

import pandas as pd
import papermill as pm 
import requests

from shared_utils import utils

dotenv.load_dotenv("_env")

TOKEN = os.environ["GITHUB_API_KEY"]
REPO = "cal-itp/data-analyses"
BRANCH = "highways-single-site"
REPO_FOLDER = "bus_service_increase/"

DEFAULT_COMMITTER = {
    "name": "Cal-ITP service user",
    "email": "hello@calitp.org",
}

notebooks_to_run = {
    # key: name of notebook in directory
    # value: name of the notebook papermill execution (can be renamed)
    "ca-highways-no-parallel-routes2.ipynb": "./ca-highways-no-parallel-routes.ipynb",
}

def publish_notebooks(notebooks_to_run):
    try:
        for input_file, output_file in notebooks_to_run.items():        
            pm.execute_notebook(
                input_file,
                output_file,
                #cwd=f"./{REPO_FOLDER}",
                #log_output=True
            )
            output_format = 'html'

            subprocess.run([
                "jupyter",
                "nbconvert",
                "--to",
                output_format,
                "--no-input",
                "--no-prompt",
                output_file,
            ]) 

            print("Converted to HTML")
            # Now find the HTML file and upload
            html_file_name = output_file.replace(".ipynb", ".html")
            #print(f"html name: {html_file_name}")

            utils.upload_file_to_github(
                TOKEN,
                REPO,
                BRANCH,
                f"{REPO_FOLDER}{html_file_name}",
                f"./{html_file_name}",
                f"Upload {html_file_name}",
                DEFAULT_COMMITTER
            )
            print("Successful upload to GitHub")
            
            os.remove(f"./{html_file_name}")
    except:
        print(f"Unsuccessful upload of {html_file_name}")
    
    
if __name__ == "__main__":
    publish_notebooks(notebooks_to_run)