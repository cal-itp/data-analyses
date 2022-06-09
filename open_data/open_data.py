"""
Track the metadata updates for all open data portal datasets.
"""
import metadata_update
import hqta_metadata_dict

OPEN_DATA = {
    "hqta_areas": {
        "path": "./metadata_xml/ca_hq_transit_areas.xml", 
        "metadata_dict": hqta_metadata_dict.HQTA_TRANSIT_AREAS_DICT
    },
    "hqta_stops": {
        "path": "./metadata_xml/ca_hq_transit_stops.xml",
        "metadata_dict": hqta_metadata_dict.HQTA_TRANSIT_STOPS_DICT
    }
}

for name, dataset in OPEN_DATA.items():
    print(name)
    print("-------------------------------------------")
    metadata_update.update_metadata_xml(dataset["path"], dataset["metadata_dict"])
