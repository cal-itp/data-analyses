# README

## Open Data Portal
1. [HQTA Areas](https://gis.data.ca.gov/datasets/863e61eacbf3463ab239beb3cee4a2c3_0): metadata   [feature server](https://gisdata.dot.ca.gov/arcgis/rest/services/CHrailroad/CA_HQ_Transit_Areas/FeatureServer) or [map server](https://gisdata.dot.ca.gov/arcgis/rest/services/CHrailroad/CA_HQ_Transit_Areas/MapServer)
1. [HQTA Stops](https://gis.data.ca.gov/datasets/f6c30480f0e84be699383192c099a6a4_0): metadata [feature server](https://gisdata.dot.ca.gov/arcgis/rest/services/CHrailroad/CA_HQ_Transit_Stops/FeatureServer) or [map server](https://gisdata.dot.ca.gov/arcgis/rest/services/CHrailroad/CA_HQ_Transit_Stops/MapServer)
1. [CA Transit Routes](https://gis.data.ca.gov/datasets/dd7cb74665a14859a59b8c31d3bc5a3e_0): metadata [feature server](https://gisdata.dot.ca.gov/arcgis/rest/services/CHrailroad/CA_Transit_Routes/FeatureServer) or [map server](https://gisdata.dot.ca.gov/arcgis/rest/services/CHrailroad/CA_Transit_Routes/MapServer)
1. [CA Transit Stops](https://gis.data.ca.gov/datasets/900992cc94ab49dbbb906d8f147c2a72_0): metadata [feature server](https://gisdata.dot.ca.gov/arcgis/rest/services/CHrailroad/CA_Transit_Stops/FeatureServer) or [map server](https://gisdata.dot.ca.gov/arcgis/rest/services/CHrailroad/CA_Transit_Stops/MapServer)
1. All GTFS datasets [metadata/data dictionary](https://data.ca.gov/dataset/cal-itp-gtfs-ingest-pipeline-dataset/resource/e26bf6ee-419d-4a95-8e4c-e2b13d5de793)


## Metadata Automation Steps and References

1. Dataset created in Hub. Run [gcs_to_esri](./gcs_to_esri.py) script to convert geoparquets to zipped shapefiles.
    * Zipped shapefiles need to be downloaded from Hub.
    * Unzip before reading it in ESRI as layers
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
    * Only use EPSG:4326 (WGS84). Use `gdf.to_crs()` if necessary. All open data portal datasets will be in WGS84.
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

## Open Data Intake Process
Open a ticket on the Intranet to update or add new services and provide [justification](./intake_justification.md)
