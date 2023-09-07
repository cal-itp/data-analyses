"""
Overwrite XML metadata using JSON.

Analyst inputs a dictionary of values to overwrite.
Convert JSON back to XML to feed in ArcGIS.
"""
import json
import pandas as pd
import xml.etree.ElementTree as ET
import xmltodict

from pathlib import Path
from typing import Union

from update_vars import DEFAULT_XML_TEMPLATE, XML_FOLDER, META_JSON

# This prefix keeps coming up, but xmltodict has trouble processing or replacing it
x = "ns0:"
main = f"{x}MD_Metadata"
char_key = "ns1:CharacterString"
dt_key = "ns1:Date"
code_key = "@codeListValue"
txt_key = "text"

def xml_to_json(path: Union[str, Path]) -> dict:  
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
        
    return d


def overwrite_default_with_dataset_elements(metadata_json: dict) -> dict:
    """
    Grab the default template.
    Replace entire sections that are dataset specific with 
    this dataset's attributes.
    """
    default_template = xml_to_json(DEFAULT_XML_TEMPLATE)
    
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


def overwrite_topic_section(metadata: dict, dataset_info: dict) -> dict: 
    """
    Overwrite the metadata for ArcPro 
    Overview > Item Description and Topic & Keywords
    and Resource > Details / Extents section(s)
    with dictionary of dataset info supplied.
    """
    d = dataset_info
    
    ## Identification Info
    id_info = metadata[f"{x}identificationInfo"][f"{x}MD_DataIdentification"]
    status_info = id_info[f"{x}status"][f"{x}MD_ProgressCode"]
    
    ## Topic & Keywords
    id_info[f"{x}topicCategory"][f"{x}MD_TopicCategoryCode"] = d["theme_topic"]
    
    # this might be GIS theme keywords?
    (id_info[f"{x}descriptiveKeywords"][0]
     [f"{x}MD_Keywords"][f"{x}keyword"][char_key]) = d["theme_topic"]
        
    ## Resource > Details - Status
    status_info[code_key] = d["status"]

    status_info[txt_key] = d["status"]
    
    ## Resource > Extents - Place
    (id_info[f"{x}extent"][f"{x}EX_Extent"]
     [f"{x}description"][char_key]) = d["place"]
    
    return metadata


def overwrite_citation_section(metadata: dict, dataset_info: dict) -> dict:
    """
    Overwrite the metadata for ArcPro 
    Overview > Citation and Citation Contacts section(s) 
    with dictionary of dataset info supplied.
    """
    d = dataset_info
    
    ## Identification Info > Citation
    id_info = metadata[f"{x}identificationInfo"][f"{x}MD_DataIdentification"]
    citation_info = id_info[f"{x}citation"][f"{x}CI_Citation"]
                    
    ## Citation Dates
    (citation_info[f"{x}date"][0][f"{x}CI_Date"]
     [f"{x}date"][dt_key]) = d["creation_date"]
    
    (citation_info[f"{x}date"][1][f"{x}CI_Date"]
     [f"{x}date"][dt_key]) = d["revision_date"]
    
    ## Citation Contacts
    (citation_info[f"{x}citedResponsibleParty"][0]
     [f"{x}CI_ResponsibleParty"][f"{x}organisationName"][char_key]) = d["contact_organization"]

    (citation_info[f"{x}citedResponsibleParty"][1]
     [f"{x}CI_ResponsibleParty"][f"{x}individualName"][char_key]) = d["contact_person"]
    
    (citation_info[f"{x}citedResponsibleParty"][1]
     [f"{x}CI_ResponsibleParty"][f"{x}organisationName"][char_key]) = d["contact_organization"]
    
    (citation_info[f"{x}citedResponsibleParty"][1]
     [f"{x}CI_ResponsibleParty"][f"{x}positionName"][char_key]) = d["publish_entity"]
    
    (citation_info[f"{x}citedResponsibleParty"][1]
     [f"{x}CI_ResponsibleParty"][f"{x}contactInfo"][f"{x}CI_Contact"]
     [f"{x}address"][f"{x}CI_Address"]
     [f"{x}electronicMailAddress"][char_key]) = d["contact_email"]
        
    return metadata


def overwrite_resource_section(metadata: dict, dataset_info: dict) -> dict:
    """
    Overwrite the metadata for ArcPro 
    Resource > Maintenance & Constraints section(s) 
    with dictionary of dataset info supplied.    
    """
    d = dataset_info
    
    ## Identification Info > Resource Maintenance / Resource Constraints
    id_info = metadata[f"{x}identificationInfo"][f"{x}MD_DataIdentification"]    
    maint_info = id_info[f"{x}resourceMaintenance"]
    constraint_info = id_info[f"{x}resourceConstraints"]
    
    ## Maintenance
    (maint_info[f"{x}MD_MaintenanceInformation"]
     [f"{x}maintenanceAndUpdateFrequency"]
     [f"{x}MD_MaintenanceFrequencyCode"][code_key]) = d["frequency"]
    
    (maint_info[f"{x}MD_MaintenanceInformation"]
     [f"{x}maintenanceAndUpdateFrequency"]
     [f"{x}MD_MaintenanceFrequencyCode"][txt_key]) = d["frequency"]    
    
    ## Constraints
    (constraint_info[0][f"{x}MD_Constraints"]
     [f"{x}useLimitation"][char_key]) = d["public_access"]        

    (constraint_info[1][f"{x}MD_LegalConstraints"]
     [f"{x}useLimitation"][char_key]) = d["boilerplate_desc"] 
    
    (constraint_info[2][f"{x}MD_LegalConstraints"]
     [f"{x}useLimitation"][char_key]) = d["boilerplate_license"] 

    return metadata
    

def overwrite_data_quality_section(metadata: dict, dataset_info: dict) -> dict:
    """
    Overwrite the metadata for ArcPro 
    Resource > Quality section(s)
    with dictionary of dataset info supplied.  
    """
    d = dataset_info
    
    qual_info = metadata[f"{x}dataQualityInfo"][f"{x}DQ_DataQuality"]
    
    ## Data Quality
    (qual_info[f"{x}report"][f"{x}DQ_RelativeInternalPositionalAccuracy"]
     [f"{x}measureDescription"][char_key]) = d["horiz_accuracy"]
    
    (qual_info[f"{x}lineage"][f"{x}LI_Lineage"]
     [f"{x}processStep"][f"{x}LI_ProcessStep"]
     [f"{x}description"][char_key]) = d["methodology"]
    
    return metadata


def overwrite_distribution_section(metadata: dict, dataset_info: dict) -> dict:
    """
    Overwrite the metadata for ArcPro 
    Resource > Distribution section(s)
    with dictionary of dataset info supplied.  
    """
    d = dataset_info
    
    dist_info = (metadata[f"{x}distributionInfo"][f"{x}MD_Distribution"]
                 [f"{x}transferOptions"][f"{x}MD_DigitalTransferOptions"]
                 [f"{x}onLine"][f"{x}CI_OnlineResource"]
                )

    ## Distribution Info
    dist_info[f"{x}linkage"][f"{x}URL"] = d["readme"]
    
    return metadata
    

def overwrite_metadata_json(metadata_json: dict, dataset_info: dict) -> dict:
    d = dataset_info
    new_metadata = metadata_json.copy()
    
    new_metadata[main] = overwrite_topic_section(new_metadata[main], d)
    new_metadata[main] = overwrite_citation_section(new_metadata[main], d)    
    new_metadata[main] = overwrite_resource_section(new_metadata[main], d)
    new_metadata[main] = overwrite_data_quality_section(new_metadata[main], d)
    new_metadata[main] = overwrite_distribution_section(new_metadata[main], d)
    
    return new_metadata 


def update_dataset_metadata_xml(
    dataset_name: str, 
    metadata_path: Union[str, Path] = META_JSON, 
    metadata_folder: Union[str, Path] = XML_FOLDER
):
    """
    xml_file: string.
        Path to the XML metadata file.
        Ex: ./data/my_metadata.xml
        
    dataset_info: dict.
        Dictionary with values to overwrite in metadata. 
        Analyst needs to replace the values where needed.
    """
    
    # (1) Read in original XML as JSON
    xml_file = Path(metadata_folder).joinpath(f"{dataset_name}.xml")
    esri_metadata = xml_to_json(xml_file)
    print("Read in XML as JSON")
    
    # (2) Read in JSON for all the metadata and grab this dataset's dict
    with open(metadata_path) as f:
        all_metadata = json.load(f)
    
    dataset_info = all_metadata[dataset_name]
    
    # (3) Apply template and bring in dataset's spatial info
    metadata_templated = overwrite_default_with_dataset_elements(esri_metadata)
    print("Default template applied.")
        
    # (4) Overwrite other portions of metadata with dictionary input
    new_metadata = overwrite_metadata_json(metadata_templated, dataset_info)
    print("Overwrite JSON using dict")

    # (5) Convert new metadata dict and write back as xml
    new_xml = xmltodict.unparse(new_metadata, encoding='utf-8', pretty=True)
    print("Convert JSON back to XML")
    
    # (6) Save new XML file in a sub-directory
    OUTPUT_FOLDER = metadata_folder.joinpath(Path("run_in_esri"))
    Path.mkdir(OUTPUT_FOLDER, exist_ok = True)
            
    with open(OUTPUT_FOLDER.joinpath(f"{dataset_name}.xml"), 'w') as f:
        f.write(new_xml)
    print("Save over existing XML")
