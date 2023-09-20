"""
Compare the FGDC template for each open data portal dataset and
fill it with the field definition and definition source 
from our yaml
"""
import xmltodict
import yaml

from pathlib import Path
from typing import Union

from open_data import RUN_ME
from metadata_update_pro import xml_to_json
from update_data_dict import unpack_list_of_tables_as_dict
from update_vars import XML_FOLDER, DATA_DICT_YML

def grab_data_dictionary_for_dataset(
    dataset_name: str,
    data_dict_file: Union[str, Path] = DATA_DICT_YML
) -> dict:
    """
    Open the data dictionary yaml and 
    unpack the table section as a dictionary. 
    """
    with open(Path(data_dict_file)) as f:
        data_dict = yaml.load(f, yaml.Loader)
        
    dict_of_tables = unpack_list_of_tables_as_dict(data_dict["tables"])

    return dict_of_tables[dataset_name]


def populate_default_esri_columns(
    new_field_metadata: dict
) -> list:
    """
    From the field section, there are default ESRI columns
    that we need to grab 
    (OBJECTID, Shape (geometry), Shape_Area or Shape_Length. 
    We want to use those in our new field metadata.
    """
    fields_list = new_field_metadata["attr"]
    
    default_attributes_list = []
    include_cols = ["OBJECTID", "Shape", "Shape_Area", "Shape_Length"]
    
    for i in fields_list:
        if i["attrlabl"] in include_cols:
            default_attributes_list.append(i) 
    
    new_field_metadata["attr"] = default_attributes_list
    
    return new_field_metadata


def populate_other_dataset_columns(
    new_field_metadata: dict
) -> dict:
    """
    Go through all the fields populated in the data dictionary yml
    and append those definitions and definition sources (if available).
    """
    fields_list = new_field_metadata["attr"]
    
    dataset_name = new_field_metadata["enttyp"]["enttypl"]
    
    our_data_dict = grab_data_dictionary_for_dataset(dataset_name)

    for field_name, field_attributes_dict in our_data_dict.items():
        new_field_dict = {}
        
        if field_name != "dataset_name":
            new_field_dict["attrlabl"] = field_name    
            new_field_dict["attrdef"] = field_attributes_dict["definition"]
        
            # If we can find definition_source key (not None), then
            # add this element
            if field_attributes_dict.get("definition_source") is not None:
                new_field_dict["attrdefs"] = field_attributes_dict["definition_source"]

            fields_list.append(new_field_dict)
    
    return new_field_metadata
    


def populate_fgdc_template_for_dataset(dataset_name: str):
    """
    Each dataset has its own FGDC template, which contains 
    spatial data info and field info.
    We will modify field section with what's stored in our
    data dictionary yml.
    """
    print(dataset_name)
    
    # Each dataset must have its own FGDC template...
    # we only want to adjust fields
    FGDC_META = XML_FOLDER.joinpath(f"{dataset_name}_fgdc.xml")
    
    metadata = xml_to_json(FGDC_META)
    
    # Key into the field section
    field_section = metadata["metadata"]["eainfo"]["detailed"]

    # Create a copy of the metadata dict so we can manipulate it.
    new_field_metadata = field_section.copy()
    new_field_metadata = populate_default_esri_columns(new_field_metadata)
    new_field_metadata = populate_other_dataset_columns(new_field_metadata)

    # Overwrite this section with our new info
    metadata["metadata"]["eainfo"]["detailed"] = new_field_metadata

    new_xml = xmltodict.unparse(metadata, encoding='utf-8', pretty=True)
    
    with open(FGDC_META, 'w') as f:
        f.write(new_xml)
    
    print(f"Save over existing XML for {dataset_name}")


if __name__ == "__main__":
    
    for d in RUN_ME:
        populate_fgdc_template_for_dataset(d)