"""
Overwrite XML metadata using JSON.

Analyst inputs a dictionary of values to overwrite.
Convert JSON back to XML to feed in ArcGIS.
"""
import os
import pandas as pd
import xml.etree.ElementTree as ET
import xmltodict

import validation_pro
from update_vars import DEFAULT_XML_TEMPLATE, XML_FOLDER

TEMPLATE_XML = f"./{XML_FOLDER}{DEFAULT_XML_TEMPLATE}"

# This prefix keeps coming up, but xmltodict has trouble processing or replacing it
x = "ns0:"
main = f"{x}MD_Metadata"


def xml_to_json(path: str) -> dict:  
    """
    Convert XML to JSON
    https://stackoverflow.com/questions/48821725/xml-parsers-expat-expaterror-not-well-formed-invalid-token
    """
    try:
        print(f"Loading XML as JSON from {path}")
        xml = ET.tostring(ET.parse(path).getroot())
        return xmltodict.parse(
            xml, 
            # this needs to be commented out for ArcGIS pro version to work
            #attr_prefix="", 
            cdata_key="text", 
            #process_namespaces=True,
            dict_constructor=dict
        )
    except:
        print(f"Loading failed for {path}")
    return {}


def lift_necessary_dataset_elements(metadata_json: dict) -> dict:
    """
    Lift necessary stuff from 1st time through shp to file gdb
    """
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
    default_template = xml_to_json(TEMPLATE_XML)
    
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


def update_arcpy_metadata_class(dataset_info: dict) -> :
    """
    This step needs to take place in ArcPro before we export XML.
    """
        
    for_arcpy = [
        "dataset_name", # title
        "theme_keywords", # tags
        "purpose", # summary
        "description", # abstract
        "public_access", # accessConstraints
    ]
    
    subset_dict = {i: dataset_info[i] for i in for_arcpy}
    
    #test_metadata.exportMetadata("test.xml")
    
    return subset_dict


def overwrite_overview(metadata: dict, dataset_info: dict) -> dict:
    """
    Overwrite the metadata for ArcPro Overview section 
    with dictionary of dataset info supplied.
    """
    d = dataset_info
    
    # This is how most values are keyed in for last dict
    key = "ns1:CharacterString"
    key_dt = "ns1:Date"
    enum = "@codeListValue"
    t = "text"
    
    id_info = metadata[f"{x}identificationInfo"][f"{x}MD_DataIdentification"]
    
    
    citation_info[f"{x}title"][key] = d["dataset_name"]  
    
    
    id_info[f"{x}abstract"][key] = d["abstract"]
    id_info[f"{x}purpose"][key] = d["purpose"]
    (id_info[f"{x}descriptiveKeywords"][0]
     [f"{x}MD_Keywords"][f"{x}keyword"]) = d["theme_keywords"]
    id_info[f"{x}topicCategory"][f"{x}MD_TopicCategoryCode"] = d["theme_topic"]
    id_info[f"{x}extent"][0][f"{x}EX_Extent"][f"{x}description"][key] = d["place"]
    
    citation_info = id_info[f"{x}citation"][f"{x}CI_Citation"]
  
    
    return metadata



def overwrite_id_info(metadata: dict, dataset_info: dict) -> dict:
    """
    Overwrite the metadata after dictionary of dataset info is supplied
    """
    d = dataset_info
    # This is how most values are keyed in for last dict
    key = "ns1:CharacterString"
    key_dt = "ns1:Date"
    enum = "@codeListValue"
    t = "text"
    
    ## Identification Info
    id_info = metadata[f"{x}identificationInfo"][f"{x}MD_DataIdentification"]
    
    id_info[f"{x}abstract"][key] = d["abstract"]
    id_info[f"{x}purpose"][key] = d["purpose"]
    (id_info[f"{x}descriptiveKeywords"][0]
     [f"{x}MD_Keywords"][f"{x}keyword"]) = d["theme_keywords"]
    id_info[f"{x}topicCategory"][f"{x}MD_TopicCategoryCode"] = d["theme_topic"]
    id_info[f"{x}extent"][0][f"{x}EX_Extent"][f"{x}description"][key] = d["place"]
    
    citation_info = id_info[f"{x}citation"][f"{x}CI_Citation"]
    citation_info[f"{x}title"][key] = d["dataset_name"]    
    
    beginning_cite = citation_info[f"{x}date"][0][f"{x}CI_Date"] 
    beginning_cite[f"{x}date"][key_dt] = d["creation_date"]
    beginning_cite[f"{x}dateType"][f"{x}CI_DateTypeCode"][enum] = "creation"
    beginning_cite[f"{x}dateType"][f"{x}CI_DateTypeCode"][t] = "creation"    
    
    end_cite = citation_info[f"{x}date"][1][f"{x}CI_Date"] 
    end_cite[f"{x}date"][key_dt] = d["beginning_date"]
    end_cite[f"{x}dateType"][f"{x}CI_DateTypeCode"][enum] = "revision"
    end_cite[f"{x}dateType"][f"{x}CI_DateTypeCode"][t] = "revision"      
        
    citation_contact = citation_info[f"{x}citedResponsibleParty"][f"{x}CI_ResponsibleParty"]
    citation_contact[f"{x}individualName"][key] = d["contact_person"]
    citation_contact[f"{x}organisationName"][key] = d["contact_organization"]  
    citation_contact[f"{x}positionName"][key] = d["publish_entity"]
    (citation_contact[f"{x}contactInfo"][f"{x}CI_Contact"][f"{x}address"]
     [f"{x}CI_Address"][f"{x}electronicMailAddress"][key]) = d["contact_email"]
    
    status_info = id_info[f"{x}status"][f"{x}MD_ProgressCode"]
    status_info[enum] = d["status"]
    status_info[t] = d["status"]
    
    maint_info = id_info[f"{x}resourceMaintenance"][f"{x}MD_MaintenanceInformation"]
    (maint_info[f"{x}maintenanceAndUpdateFrequency"]
     [f"{x}MD_MaintenanceFrequencyCode"][enum]) = d["frequency"]
    (maint_info[f"{x}maintenanceAndUpdateFrequency"]
     [f"{x}MD_MaintenanceFrequencyCode"][t]) = d["frequency"]
    maint_info[f"{x}dateOfNextUpdate"][key_dt] = d["end_date"]
    maint_info = overwrite_contact_info(maint_info, d)
        
    (id_info[f"{x}resourceConstraints"][0][f"{x}MD_LegalConstraints"]
     [f"{x}useLimitation"][key]) = d["public_access"]   
    (id_info[f"{x}resourceConstraints"][1][f"{x}MD_LegalConstraints"]
     [f"{x}useLimitation"][key]) = d["boilerplate_desc"]
    (id_info[f"{x}resourceConstraints"][2][f"{x}MD_LegalConstraints"]
     [f"{x}useLimitation"][key]) = d["boilerplate_license"]   
    (id_info[f"{x}resourceConstraints"][2][f"{x}MD_LegalConstraints"]
     [f"{x}useConstraints"][f"{x}MD_RestrictionCode"][enum]) = "license"
    (id_info[f"{x}resourceConstraints"][2][f"{x}MD_LegalConstraints"]
     [f"{x}useConstraints"][f"{x}MD_RestrictionCode"][t]) = "license"    

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
        
    new_metadata[main] = overwrite_data_quality_info(new_metadata[main], d)
                
    #m["eainfo"]["detailed"]["enttyp"]["enttypd"] = d["data_dict_type"]    
    #m["eainfo"]["detailed"]["enttyp"]["enttypds"] = d["data_dict_url"]    
      
    return new_metadata 


def update_metadata_xml(
    xml_file: str, 
    dataset_info: dict, 
    metadata_folder: str = XML_FOLDER
):
    """
    xml_file: string.
        Path to the XML metadata file.
        Ex: ./data/my_metadata.xml
        
    dataset_info: dict.
        Dictionary with values to overwrite in metadata. 
        Analyst needs to replace the values where needed.
    """
    
    # Read in original XML as JSON
    esri_metadata = xml_to_json(xml_file)
    print("Read in XML as JSON")
    
    # Apply template
    metadata_templated = overwrite_default_with_dataset_elements(esri_metadata)
    print("Default template applied.")

    # These rely on functions, so they can't be used in pydantic easily
    dataset_info = validation_pro.fix_values_in_validated_dict(dataset_info) 
    
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
