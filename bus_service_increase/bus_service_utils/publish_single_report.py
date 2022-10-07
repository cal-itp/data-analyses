"""
Script to publish single notebooks,
convert ipynb to html with nbconvert
"""
import pandas as pd
import papermill as pm 
import requests
import subprocess

from shared_utils import utils

REPORT_FOLDER = "img/"

notebooks_to_run = {
    # key: name of notebook in directory
    # value: name of the notebook papermill execution (can be renamed)
    "highways-uncompetitive-routes.ipynb": "highways-uncompetitive-routes.ipynb"
}

def publish_notebooks(notebooks_to_run):
    for input_file, output_file in notebooks_to_run.items():        
        pm.execute_notebook(
            input_file,
            output_file,
            #cwd=f"./{REPORT_FOLDER}/",
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
    
        # Move the html file into the same folder as where it's stored 
        # for GH pages rendering
        # Can't move before because functions won't import 
        os.rename(f"{html_file_name}", f"{REPORT_FOLDER}{html_file_name}")
        
        # Clean up - since I renamed file, I don't want executed notebook 
        # to stay in directory
        if input_file != output_file:
            os.remove(output_file)
    
    
if __name__ == "__main__":
    publish_notebooks(notebooks_to_run)