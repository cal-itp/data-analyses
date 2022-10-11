"""
Overwrite XML metadata using JSON.

Analyst inputs a dictionary of values to overwrite.
Convert JSON back to XML to feed in ArcGIS.
"""
import os
import pandas as pd
import xml.etree.ElementTree as ET
import xmltodict

from pydantic import BaseModel
from typing import List, Dict

import validation 

METADATA_FOLDER = "metadata_xml/"

# Convert XML to JSON
# https://stackoverflow.com/questions/48821725/xml-parsers-expat-expaterror-not-well-formed-invalid-token
def xml_to_json(path: str) -> dict:  
    try:
        print(f"Loading XML as JSON from {path}")
        xml = ET.tostring(ET.parse(path).getroot())
        return xmltodict.parse(xml, 
                               attr_prefix="", cdata_key="text", 
                               #process_namespaces=True,
                               dict_constructor=dict)
    except:
        print(f"Loading failed for {path}")
    return {}


# Lift necessary stuff from 1st time through shp to file gdb
def lift_necessary_dataset_elements(metadata_json: dict) -> dict:
    m = metadata_json["ns0:MD_Metadata"]
    
    # Store this info in a dictionary
    d = {}
        
    # Date Stamp
    d["ns0:dateStamp"] = m["ns0:dateStamp"] 
    
    # Spatial Representation Info
    d["ns0:spatialRepresentationInfo"] = m["ns0:spatialRepresentationInfo"] 
   
    # Coordinate Reference System Info
    d["ns0:referenceSystemInfo"] = m["ns0:referenceSystemInfo"] 
    
    # Distribution Info
    d["ns0:distributionInfo"] = m["ns0:distributionInfo"]   
    
    return d


def overwrite_default_with_dataset_elements(metadata_json: dict) -> dict:
    DEFAULT_XML = f"./{METADATA_FOLDER}default_pro.xml"
    default_template = xml_to_json(DEFAULT_XML)
    default = default_template["ns0:MD_Metadata"]
    
    # Grab the necessary elements from my dataset
    necessary_elements = lift_necessary_dataset_elements(metadata_json)
    
    # Overwrite it in the default template
    for key, value in default.items():
        if key in necessary_elements.keys():
            default[key] = necessary_elements[key]     
            
    # Return the default template, but now with our dataset's info populated
    return default_template


# Validate the metadata dictionary input we supply
# Certain fields are pre-filled, unlikely to change
# If the key isn't there, then it'll be filled in with default
class metadata_input(BaseModel):
    dataset_name: str
    publish_entity: str = "California Integrated Travel Project"
    abstract: str
    purpose: str
    beginning_date: str
    end_date: str
    place: str = "California"
    status: str = "Complete"
    frequency: str = "Monthly"
    theme_topics: Dict
    methodology: str
    data_dict_type: str
    data_dict_url: str
    contact_organization: str = "Caltrans"
    contact_person: str
    contact_email: str = "hello@calitp.org"
    horiz_accuracy: str = "4 meters"
    

def fix_values_in_validated_dict(d: dict) -> dict:
    # Construct the theme_topics dict from keyword list
    d["theme_topics"] = validation.fill_in_keyword_list(
        topic="transportation", keyword_list=d["theme_keywords"])
    
    d["frequency"] = validation.check_update_frequency(d["frequency"])
    
    d["data_dict_type"] = validation.check_data_dict_format(d["data_dict_type"])
    
    d["beginning_date"] = validation.check_dates(d["beginning_date"])
    d["end_date"] = validation.check_dates(d["end_date"])
    
    d["horiz_accuracy"] = validation.check_horiz_accuracy(d["horiz_accuracy"])
    
    return d


# Overwrite the metadata after dictionary of dataset info is supplied
def overwrite_metadata_json(metadata_json: dict, dataset_info: dict) -> dict:
    d = dataset_info
    new_metadata = metadata_json.copy()
    m = new_metadata["metadata"]

    
    return new_metadata 



def update_metadata_xml(xml_file: str, dataset_info: dict, 
                        first_run: bool = False, 
                        metadata_folder: str = METADATA_FOLDER):
    """
    xml_file: string.
        Path to the XML metadata file.
        Ex: ./data/my_metadata.xml
        
    dataset_info: dict.
        Dictionary with values to overwrite in metadata. 
        Analyst needs to replace the values where needed.
    
    first_run: boolean.
        Defaults to False.
        For the first time, set to True, so you apply `default.xml` as template.
    """
    
    # Read in original XML as JSON
    esri_metadata = xml_to_json(xml_file)
    print("Read in XML as JSON")
    
    if first_run is True:
        # Apply template
        metadata_templated = overwrite_default_with_dataset_elements(esri_metadata)
        print("Default template applied.")
    else:
        metadata_templated = esri_metadata.copy()
        print("Skip default template.")
    
    # These rely on functions, so they can't be used in pydantic easily
    dataset_info = fix_values_in_validated_dict(dataset_info) 
    
    # Validate the dict input with pydantic
    DATASET_INFO_VALIDATED = metadata_input(**dataset_info).dict()
    
    # Overwrite the metadata with dictionary input
    new_metadata = overwrite_metadata_json(metadata_templated, DATASET_INFO_VALIDATED)
    print("Overwrite JSON using dict")

    new_xml = xmltodict.unparse(new_metadata, pretty=True)
    print("Convert JSON back to XML")
    
    # Overwrite existing XML file
    OUTPUT_FOLDER = "run_in_esri/"
    if not os.path.exists(f"./{metadata_folder}{OUTPUT_FOLDER}"):
        os.makedirs(f"./{metadata_folder}{OUTPUT_FOLDER}")
    
    FILE = f"{xml_file.split(metadata_folder)[1]}"
        
    with open(f"./{metadata_folder}{OUTPUT_FOLDER}{FILE}", 'w') as f:
        f.write(new_xml)
    print("Save over existing XML")
