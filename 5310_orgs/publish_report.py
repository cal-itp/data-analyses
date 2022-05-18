'''
single report script for 5310
run in data-analyses/5310_orgs
'''
import dotenv
import os
import subprocess
import sys

import pandas as pd
import papermill as pm 
import requests

from shared_utils import utils


OUTPUT_FILENAME = "5310_report_output"

pm.execute_notebook(
    # notebook to execute
    '5310_report_work.ipynb',
    # if needed, rename the notebook as something different
    # this will be the filename that is used when converting to HTML or PDF
    f'{OUTPUT_FILENAME}.ipynb',
)

# shell out, run NB Convert
OUTPUT_FORMAT = 'html'
subprocess.run([
    "jupyter",
    "nbconvert",
    "--to",
    OUTPUT_FORMAT,
    "--no-input",
    "--no-prompt",
    f"{OUTPUT_FILENAME}.ipynb",
])