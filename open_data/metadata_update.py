"""
Overwrite XML metadata using JSON.

Analyst inputs a dictionary of values to overwrite.
Convert JSON back to XML to feed in ArcGIS.
"""
import xml.etree.ElementTree as ET
import xmltodict

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
def lift_necessary_dataset_elements(metadata_json):
    m = metadata_json["metadata"]
    
    # Store this info in a dictionary
    d = {}
    
    # Bounding box
    d["idinfo"] = {}
    d["idinfo"]["spdom"] = m["idinfo"]["spdom"] 
    
    # Some description about geospatial layer
    d["spdoinfo"] = m["spdoinfo"] 
    
    # Spatial reference info
    d["spref"] = m["spref"] 
    
    # Field and entity attributes
    d["eainfo"] = m["eainfo"]
    
    return d


def overwrite_default_with_dataset_elements(metadata_json):
    DEFAULT_XML = f"./{METADATA_FOLDER}default.xml"
    default_template = xml_to_json(DEFAULT_XML)
    default = default_template["metadata"]
    
    # Grab the necessary elements from my dataset
    necessary_elements = lift_necessary_dataset_elements(metadata_json)
    
    # Overwrite it in the default template
    for key, value in default.items():
        if key in necessary_elements.keys() and key != "idinfo":
            default[key] = necessary_elements[key]
        
        elif key == "idinfo":
            for k, v in value.items():
                if k == "spdom":
                    default[key][k] = necessary_elements[key][k]
           
            
    # Return the default template, but now with our dataset's info populated
    return default_template


# Function to put in list of keywords (MINIMUM 5 needed)
def fill_in_keyword_list(topic='transportation', keyword_list = []):
    if len(keyword_list) >= 5:
        filled_out_list = [
            {'themekt': 'ISO 19115 Topic Categories',
             'themekey': topic},
             {'themekt': 'None',
              'themekey': keyword_list
             }
        ]

        return filled_out_list
    else:
        return "Input minimum 5 keywords"

    
# First time metadata is generated off of template, it holds '-999' as value
# Subsequent updates, pull it, and add 1
def check_edition_add_one(metadata):
    input_edition = metadata["idinfo"]["citation"]["citeinfo"]["edition"]
    
    if input_edition == '-999':
        new_edition = str(1)
    else:
        new_edition = str(int(input_edition) + 1)
    
    return new_edition
    

SAMPLE_DATASET_INFO = {
    "dataset_name": "my_dataset", 
    "publish_entity": "California Integrated Travel Project", 

    "abstract": "Public. EPSG: 3310",
    "purpose": "Summary sentence about dataset.", 

    "beginning_date": "YYYYMMDD",
    "end_date": "YYYYMMDD",
    "place": "California",

    "status": "Complete", 
    "frequency": "Monthly",
    
    "theme_topics": [], 

    "methodology": "Detailed methodology description", 
    
    "data_dict_type": "CSV",
    "data_dict_url": "some_url", 

    "contact_organization": "Caltrans", 
    "contact_person": "Analyst Name", 
    "contact_email": "hello@calitp.org" 
}


# Overwrite the metadata after dictionary of dataset info is supplied
def overwrite_metadata_json(metadata_json, DATASET_INFO):
    d = DATASET_INFO
    new_metadata = metadata_json.copy()
    m = new_metadata["metadata"]
    
    m["idinfo"]["citation"]["citeinfo"]["title"] = d["dataset_name"]
    m["idinfo"]["citation"]["citeinfo"]["pubinfo"]["publish"] = d["publish_entity"]
    ## Need edition and resource contact added to be approved 
    # Add edition 
    # Use number instead of date (shows up when exported in FGDC)
    NEW_EDITION = check_edition_add_one(m)
    m["idinfo"]["citation"]["citeinfo"]["edition"] = NEW_EDITION
    
    m["idinfo"]["descript"]["abstract"] = d["abstract"]
    m["idinfo"]["descript"]["purpose"] = d["purpose"]
    
    m["idinfo"]["timeperd"]["timeinfo"]["rngdates"]["begdate"] = d["beginning_date"]
    m["idinfo"]["timeperd"]["timeinfo"]["rngdates"]["enddate"] = d["end_date"]
    m["idinfo"]["timeperd"]["current"] = d["place"]
    
    m["idinfo"]["status"]["progress"] = d["status"]
    m["idinfo"]["status"]["update"] = d["frequency"]

    m["idinfo"]["keywords"] = d["theme_topics"]    

    # Add resource contact
    m["idinfo"]["ptcontac"]["cntinfo"]["cntorgp"]["cntorg"] = d["contact_organization"]
    m["idinfo"]["ptcontac"]["cntinfo"]["cntorgp"]["cntper"] = d["contact_person"]
    m["idinfo"]["ptcontac"]["cntinfo"]["cntpos"] = d["publish_entity"]
    m["idinfo"]["ptcontac"]["cntinfo"]["cntemail"] = d["contact_email"]    
    
    m["dataqual"]["lineage"]["procstep"]["procdesc"] = d["methodology"]    
    
    m["eainfo"]["detailed"]["enttyp"]["enttypl"] = d["dataset_name"]    
    m["eainfo"]["detailed"]["enttyp"]["enttypd"] = d["data_dict_type"]    
    m["eainfo"]["detailed"]["enttyp"]["enttypds"] = d["data_dict_url"]    
  
    m["metainfo"]["metc"]["cntinfo"]["cntorgp"]["cntorg"] = d["contact_organization"]    
    m["metainfo"]["metc"]["cntinfo"]["cntorgp"]["cntper"] = d["contact_person"]    
    m["metainfo"]["metc"]["cntinfo"]["cntpos"] = d["publish_entity"]    
    m["metainfo"]["metc"]["cntinfo"]["cntemail"] = d["contact_email"]    

    return new_metadata 



def update_metadata_xml(XML_FILE, DATASET_INFO = SAMPLE_DATASET_INFO, first_run=False):
    """
    XML_FILE: string.
        Path to the XML metadata file.
        Ex: ./data/my_metadata.xml
        
    DATASET_INFO: dict.
        Dictionary with values to overwrite in metadata. 
        Analyst needs to replace the values where needed.
    
    first_run: boolean.
        Defaults to False.
        For the first time, set to True, so you apply `default.xml` as template.
    """
    
    # Read in original XML as JSON
    esri_metadata = xml_to_json(XML_FILE)
    print("Read in XML as JSON")
    
    if first_run is True:
        # Apply template
        metadata_templated = overwrite_default_with_dataset_elements(esri_metadata)
        print("Default template applied.")
    else:
        metadata_templated = esri_metadata.copy()
        print("Skip default template.")
        
    new_metadata = overwrite_metadata_json(metadata_templated, DATASET_INFO)
    print("Overwrite JSON using dict")

    new_xml = xmltodict.unparse(new_metadata, pretty=True)
    print("Convert JSON back to XML")
    
    # Overwrite existing XML file
    OUTPUT_FOLDER = "run_in_esri/"
    FILE = f"{XML_FILE.split(METADATA_FOLDER)[1]}"
        
    with open(f"{METADATA_FOLDER}{OUTPUT_FOLDER}{FILE}", 'w') as f:
        f.write(new_xml)
    print("Save over existing XML")
