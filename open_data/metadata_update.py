"""
Overwrite XML metadata using JSON.

Analyst inputs a dictionary of values to overwrite.
Convert JSON back to XML to feed in ArcGIS.
"""
import xml.etree.ElementTree as ET
import xmltodict

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
    
    m["idinfo"]["descript"]["abstract"] = d["abstract"]
    m["idinfo"]["descript"]["purpose"] = d["purpose"]
    
    m["idinfo"]["timeperd"]["timeinfo"]["rngdates"]["begdate"] = d["beginning_date"]
    m["idinfo"]["timeperd"]["timeinfo"]["rngdates"]["enddate"] = d["end_date"]
    m["idinfo"]["timeperd"]["current"] = d["place"]
    
    m["idinfo"]["status"]["progress"] = d["status"]
    m["idinfo"]["status"]["update"] = d["frequency"]

    m["idinfo"]["keywords"] = d["theme_topics"]    

    m["dataqual"]["lineage"]["procstep"]["procdesc"] = d["methodology"]    
    
    m["eainfo"]["detailed"][0]["enttyp"]["enttypl"] = d["dataset_name"]    
    m["eainfo"]["detailed"][1]["enttyp"]["enttypd"] = d["data_dict_type"]    
    m["eainfo"]["detailed"][1]["enttyp"]["enttypds"] = d["data_dict_url"]    
  
    m["metainfo"]["metc"]["cntinfo"]["cntorgp"]["cntorg"] = d["contact_organization"]    
    m["metainfo"]["metc"]["cntinfo"]["cntorgp"]["cntper"] = d["contact_person"]    
    m["metainfo"]["metc"]["cntinfo"]["cntpos"] = d["publish_entity"]    
    m["metainfo"]["metc"]["cntinfo"]["cntemail"] = d["contact_email"]    

    return new_metadata 


def update_metadata_xml(XML_FILE, DATASET_INFO = SAMPLE_DATASET_INFO):
    """
    XML_FILE: string.
        Path to the XML metadata file.
        Ex: ./data/my_metadata.xml
        
    DATASET_INFO: dict.
        Dictionary with values to overwrite in metadata. 
        Analyst needs to replace the values where needed.
    """
    
    # Read in original XML as JSON
    esri_metadata = xml_to_json(XML_FILE)
    print("Read in XML as JSON")
    
    new_metadata = overwrite_metadata_json(esri_metadata, DATASET_INFO)
    print("Overwrite JSON using dict")

    new_xml = xmltodict.unparse(new_metadata, pretty=True)
    print("Convert JSON back to XML")
    
    # Overwrite existing XML file
    with open(XML_FILE, 'w') as f:
        f.write(new_xml)
    print("Save over existing XML")
