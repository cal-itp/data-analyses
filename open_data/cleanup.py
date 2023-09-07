"""
Clean up and remove local files created in the process.
"""
import os
from pathlib import Path

import open_data
from update_vars import XML_FOLDER
from gcs_to_esri import remove_zipped_shapefiles

if __name__=="__main__":
    # Delete the XML before it was updated, and move the
    # updated XML out of the run_in_esri/ directory
    for d in open_data.RUN_ME:
        os.replace(
            XML_FOLDER.joinpath("run_in_esri", f"{d}.xml"),
            XML_FOLDER.joinpath(f"{d}.xml")
        )
    
    # Clean up local files
    remove_zipped_shapefiles()
    print("Remove local zipped shapefiles")