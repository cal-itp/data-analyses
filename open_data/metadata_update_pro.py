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
from typing import List, Dict, Literal

import validation_pro

METADATA_FOLDER = "metadata_xml/"

# This prefix keeps coming up, but xmltodict has trouble processing or replacing it
x = "ns0:"
main = f"{x}MD_Metadata"

# Convert XML to JSON
# https://stackoverflow.com/questions/48821725/xml-parsers-expat-expaterror-not-well-formed-invalid-token
def xml_to_json(path: str) -> dict:  
    try:
        print(f"Loading XML as JSON from {path}")
        xml = ET.tostring(ET.parse(path).getroot())
        return xmltodict.parse(xml, 
                               #attr_prefix="", 
                               cdata_key="text", 
                               #process_namespaces=True,
                               dict_constructor=dict)
    except:
        print(f"Loading failed for {path}")
    return {}


# Lift necessary stuff from 1st time through shp to file gdb
def lift_necessary_dataset_elements(metadata_json: dict) -> dict:
    m = metadata_json
    
    # Store this info in a dictionary
    d = {}
        
    # Date Stamp
    d[f"{x}dateStamp"] = m[f"{x}dateStamp"]
    
    # Spatial Representation Info
    d[f"{x}spatialRepresentationInfo"] = m[f"{x}spatialRepresentationInfo"] 
   
    # Coordinate Reference System Info
    d[f"{x}referenceSystemInfo"] = m[f"{x}referenceSystemInfo"] 
    
    # Distribution Info
    d[f"{x}distributionInfo"] = m[f"{x}distributionInfo"]   
    
    return d


def overwrite_default_with_dataset_elements(metadata_json: dict) -> dict:
    DEFAULT_XML = f"./{METADATA_FOLDER}default_pro.xml"
    default_template = xml_to_json(DEFAULT_XML)
    
    # Grab the necessary elements from my dataset
    necessary_elements = lift_necessary_dataset_elements(metadata_json[main])
    
    # Overwrite it in the default template
    for key, value in default_template[main].items():
        if key in necessary_elements.keys():
            default_template[main][key] = necessary_elements[key]    
        else:
            default_template[main][key] = default_template[main][key]
            
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
    status: Literal["completed", "historicalArchive", "obsolete", 
                    "onGoing", "planned", "required", 
                    "underDevelopment"] = "completed"
    frequency: Literal["continual", "daily", "weekly",
                       "fortnightly", "monthly", "quarterly", 
                       "biannually", "annually", 
                       "asNeeded", "irregular", "notPlanned", 
                       "unknown"] = "monthly"
    theme_topic: str = "transportation"
    theme_keywords: list    
    methodology: str
    #data_dict_type: str
    #data_dict_url: str
    contact_organization: str = "Caltrans"
    contact_person: str
    contact_email: str = "hello@calitp.org"
    horiz_accuracy: str = "4 meters"
    edition: str
    

def fix_values_in_validated_dict(d: dict) -> dict:
    d["theme_keywords"] = validation_pro.fill_in_keyword_list(d["theme_keywords"])
    
    d["frequency"] = validation_pro.check_update_frequency(d["frequency"])
    
    #d["data_dict_type"] = validation.check_data_dict_format(d["data_dict_type"])
    d["edition"] = validation_pro.add_edition()
    
    d["beginning_date"] = validation_pro.check_dates(d["beginning_date"])
    d["end_date"] = validation_pro.check_dates(d["end_date"])
    
    # Can we get away with 4 meters in EPSG:4326?
    #d["horiz_accuracy"] = validation.check_horiz_accuracy(d["horiz_accuracy"])
    
    return d


# Overwrite the metadata after dictionary of dataset info is supplied
def overwrite_id_info(metadata: dict, dataset_info: dict) -> dict:
    d = dataset_info
    # This is how most values are keyed in for last dict
    key = "ns1:CharacterString"
    key_dt = "ns1:Date"
    
    ## Identification Info
    id_info = metadata[f"{x}identificationInfo"][f"{x}MD_DataIdentification"]
    
    id_info[f"{x}abstract"][key] = d["abstract"]
    id_info[f"{x}purpose"][key] = d["purpose"]
    (id_info[f"{x}descriptiveKeywords"][1]
     [f"{x}MD_Keywords"][f"{x}keyword"]) = d["theme_keywords"]
    id_info[f"{x}topicCategory"][f"{x}MD_TopicCategoryCode"] = d["theme_topic"]
    id_info[f"{x}extent"][f"{x}EX_Extent"][f"{x}description"][key] = d["place"]

    
    citation_info = id_info[f"{x}citation"][f"{x}CI_Citation"]
    citation_info[f"{x}title"][key] = d["dataset_name"]
    citation_info[f"{x}date"][f"{x}CI_Date"][f"{x}date"][key_dt] = d["beginning_date"]
    citation_info[f"{x}edition"][key] = d["edition"]
    
    citation_contact = citation_info[f"{x}citedResponsibleParty"][f"{x}CI_ResponsibleParty"]
    citation_contact[f"{x}individualName"][key] = d["contact_person"]
    citation_contact[f"{x}organisationName"][key] = d["contact_organization"]  
    citation_contact[f"{x}positionName"][key] = d["publish_entity"]
    (citation_contact[f"{x}contactInfo"][f"{x}CI_Contact"][f"{x}address"]
     [f"{x}CI_Address"][f"{x}electronicMailAddress"][key]) = d["contact_email"]
    
    status_info = id_info[f"{x}status"][f"{x}MD_ProgressCode"]
    status_info["codeListValue"] = d["status"]
    status_info["text"] = d["status"]
    
    maint_info = id_info[f"{x}resourceMaintenance"][f"{x}MD_MaintenanceInformation"]
    (maint_info[f"{x}maintenanceAndUpdateFrequency"]
     [f"{x}MD_MaintenanceFrequencyCode"]["codeListValue"]) = d["frequency"]
    (maint_info[f"{x}maintenanceAndUpdateFrequency"]
     [f"{x}MD_MaintenanceFrequencyCode"]["text"]) = d["frequency"]
    maint_info[f"{x}dateOfNextUpdate"][key_dt] = d["end_date"]
    
    extent_info = (id_info[f"{x}extent"][f"{x}EX_Extent"]
                   [f"{x}temporalElement"][f"{x}EX_TemporalExtent"]
                   [f"{x}extent"]["ns2:TimePeriod"])
 
    extent_info["ns2:beginPosition"] = d["beginning_date"] + "T00:00:00"
    extent_info["ns2:endPosition"] = d["end_date"] + "T00:00:00"
    
    return metadata
    
    
def overwrite_contact_info(metadata: dict, dataset_info: dict) -> dict: 
    d = dataset_info
    key = "ns1:CharacterString"

    ## Contact Info
    contact_info = metadata[f"{x}contact"][f"{x}CI_ResponsibleParty"]
    
    contact_info[f"{x}positionName"][key] = d["publish_entity"]
    contact_info[f"{x}organisationName"][key] = d["contact_organization"]
    contact_info[f"{x}individualName"][key] = d["contact_person"]
    
    (contact_info[f"{x}contactInfo"][f"{x}CI_Contact"]
     [f"{x}address"][f"{x}CI_Address"]
     [f"{x}electronicMailAddress"][key]) = d["contact_email"] 
    
    return metadata


def overwrite_data_quality_info(metadata: dict, dataset_info: dict) -> dict:
    d = dataset_info
    key = "ns1:CharacterString"
    
    ## Data Quality
    data_qual_info = metadata[f"{x}dataQualityInfo"][f"{x}DQ_DataQuality"]
    (data_qual_info[f"{x}report"][f"{x}DQ_RelativeInternalPositionalAccuracy"]
     [f"{x}measureDescription"][key]) = d["horiz_accuracy"]
    
    (data_qual_info[f"{x}lineage"][f"{x}LI_Lineage"]
     [f"{x}processStep"][f"{x}LI_ProcessStep"]
     [f"{x}description"][key]) = d["methodology"]
    
    return metadata
    

def overwrite_metadata_json(metadata_json: dict, 
                            dataset_info: dict, first_run: bool = False) -> dict:
    d = dataset_info
    new_metadata = metadata_json.copy()
    
    new_metadata[main] = overwrite_id_info(new_metadata[main], d)
    new_metadata[main] = overwrite_contact_info(new_metadata[main], d)
    new_metadata[main] = overwrite_data_quality_info(new_metadata[main], d)
                
    #m["eainfo"]["detailed"]["enttyp"]["enttypd"] = d["data_dict_type"]    
    #m["eainfo"]["detailed"]["enttyp"]["enttypds"] = d["data_dict_url"]    
      
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
    
    if first_run:
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

    new_xml = xmltodict.unparse(new_metadata, encoding='utf-8', pretty=True)
    print("Convert JSON back to XML")
    
    # Overwrite existing XML file
    OUTPUT_FOLDER = "run_in_esri/"
    if not os.path.exists(f"./{metadata_folder}{OUTPUT_FOLDER}"):
        os.makedirs(f"./{metadata_folder}{OUTPUT_FOLDER}")
    
    FILE = f"{xml_file.split(metadata_folder)[1]}"
        
    with open(f"./{metadata_folder}{OUTPUT_FOLDER}{FILE}", 'w') as f:
        f.write(new_xml)
    print("Save over existing XML")
