import papermill as pm
import os
import subprocess


for caltrans_district in range(1, 3):
    pm.execute_notebook(
        './prototype-district.ipynb',
        output_path = f'./reports/district{caltrans_district}.ipynb',
        parameters = dict(district = caltrans_district)
    )

    print("Ran notebook")

    # shell out, run NB Convert 
    output_format = 'html'
    subprocess.run([
        "jupyter",
        "nbconvert",
        "--to",
        output_format,
        "--no-input",
        "--no-prompt",
        f'./reports/district{caltrans_district}.ipynb',
    ]) 
    
    #os.remove(f'./reports/district{caltrans_district}.ipynb')
