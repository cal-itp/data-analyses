# README
## Metadata Automation Steps and References

1. Dataset created in Hub. Save as shapefile. Do we version one as geoparquet in GCS? The file needs to be downloaded locally, so if a zipped shapefile is produced (faster to download from Hub), it will need to be unzipped before running the script. 
2. Shapefile written as feature class in file gdb
* Start with [arcgis_script](./arcgis_script.py)
* [Edit metadata for many ArcGIS items](https://desktop.arcgis.com/en/arcmap/latest/manage-data/metadata/editing-metadata-for-many-arcgis-items.htm)
* [Metadata template -- is this needed?](https://desktop.arcgis.com/en/arcmap/latest/manage-data/metadata/creating-a-metadata-template.htm)
* [Convert shp to gdb](https://gis.stackexchange.com/questions/269701/copying-multiple-shp-files-to-a-file-geodatabase)
* [Export features to gdb](https://gis.stackexchange.com/questions/366054/export-features-to-geodatabase-created-in-same-python-script)
3. Export metadata associated with feature class as XML
4. [Convert XML to JSON using xmltodict](https://stackoverflow.com/questions/48821725/xml-parsers-expat-expaterror-not-well-formed-invalid-token)
5. Supply a dictionary with the relevant info 
* Create a Python script to store dictionary (ex: `hqta.py`)
6. Overwrite values with dictionary in the JSON 
7. [Convert JSON back to XML](https://gis.stackexchange.com/questions/202978/converting-xml-dict-xml-using-python) and overwrite XML
8. Overwrite the XML and import this, overwrite/sync this metadata with feature class
9. Zip the file gdb

## Analyst Steps
1. Add your dataset to `catalog.yml` and run `gcs_to_esri`.
    * In terminal: cd `open_data` followed by `python gcs_to_esri.py` 
    * The log will show basics like column names and EPSG. Make sure the metadata reflects the same info!
    * Download the zipped shapefiles from the Hub to your local filesystem.
1. Run [arcgis_script](./arcgis_script.py) for Steps 2-3.
    * Open a notebook in Hub and find the `ARCGIS_PATH`
    * Hardcode that path for `arcpy.env.workspace = ARCGIS_PATH`
    * The exported XML metadata will be in file gdb directory.
    * Upload the XML metadata into Hub in `open_data/metadata_xml/`.
1. Open `open_data.py` and modify the script to overwrite XML for the desired datasets.
1. In terminal: `python open_data.py` for Steps 4-7.
    * Change into the `open_data` directory: `cd open_data/`.
    * The overwritten XML is stored in `open_data/metadata_xml/run_in_esri/`.
    * Download the overwritten XML files locally to run in ArcGIS.
1. Run [arcgis_script](./arcgis_script.py) for Step 8.
1. Zip the file gdb manually - can't get zipping file gdb code to work

## Open Data Portal Datasets
1. [High Quality Transit Areas (HQTA)](./hqta.py)
1. [Transit Stops and Routes (Traffic Ops request)](./traffic_ops.py)