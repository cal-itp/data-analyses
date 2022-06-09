# README
## Metadata Automation Steps and References

1. Dataset created in Hub. Save as shapefile. Do we version one as geoparquet in GCS?
2. Shapefile written as feature class in file gdb
* Start with [arcgis_script](./arcgis_script.py)
* [Edit metadata for many ArcGIS items](https://desktop.arcgis.com/en/arcmap/latest/manage-data/metadata/editing-metadata-for-many-arcgis-items.htm)
* [Metadata template -- is this needed?](https://desktop.arcgis.com/en/arcmap/latest/manage-data/metadata/creating-a-metadata-template.htm)
* [Convert shp to gdb](https://gis.stackexchange.com/questions/269701/copying-multiple-shp-files-to-a-file-geodatabase)
* [export features to gdb](https://gis.stackexchange.com/questions/366054/export-features-to-geodatabase-created-in-same-python-script)
3. Export metadata associated with feature class as XML
4. [Convert XML to JSON using xmltodict](https://stackoverflow.com/questions/48821725/xml-parsers-expat-expaterror-not-well-formed-invalid-token)
5. Supply a dictionary with the relevant info 
* Create a Python script to store dictionary (ex: `hqta_metadata_dict.py`)
6. Overwrite values with dictionary in the JSON 
7. [Convert JSON back to XML](https://gis.stackexchange.com/questions/202978/converting-xml-dict-xml-using-python) and overwrite XML
8. Overwrite the XML and import this, overwrite/sync this metadata with feature class
9. Zip the file gdb

## Analyst Steps
1. Make sure geodataframe is saved as a shapefile. Use `shared_utils.utils.make_shapefile()` 
1. Run [arcgis_script](./arcgis_script.py) for Steps 2-3.
1. In terminal: `python open_data.py` for Steps 4-7.
1. Run [arcgis_script](./arcgis_script.py) for Step 8.
1. Zip the file gdb manually - have code, check if it works in ArcGIS

## Open Data Portal Datasets
1. [High Quality Transit Areas (HQTA)](./hqta_metadata_dict.py)
