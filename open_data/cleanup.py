"""
Clean up and remove local files created in the process.
"""
from gcs_to_esri import remove_zipped_shapefiles

if __name__=="__main__":
    
    # Clean up local files
    remove_zipped_shapefiles()
    print("Remove local zipped shapefiles")