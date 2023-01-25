"""
Clean up and remove local files created in the process.
"""
import os

import open_data
from gcs_to_esri import remove_zipped_shapefiles

def delete_old_xml_check_in_new_xml(open_data_dict: dict):
    """
    After the XML file has been moved locally to sync with
    ESRI, no longer need 2 versions of the XML:
    
    (1) metadata_xml/ 
    (2) metadata_xml/run_in_esri/
    
    Just move the one from metadata_xml/run_in_esri/ out and
    check that one into GitHub.
    """

    for key, value in open_data_dict.items():
        file_path = value["path"]
        file_name = os.path.basename(file_path)
        os.replace(f"./metadata_xml/run_in_esri/{file_name}",
            f"./metadata_xml/{file_name}")
        


if __name__=="__main__":
    # Delete the XML before it was updated, and move the
    # updated XML out of the run_in_esri/ directory
    delete_old_xml_check_in_new_xml(open_data.OPEN_DATA)
    
    # Clean up local files
    remove_zipped_shapefiles()
    print("Remove local zipped shapefiles")