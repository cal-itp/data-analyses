# README

## Open Data Portal
1. [HQTA Areas](https://gis.data.ca.gov/datasets/863e61eacbf3463ab239beb3cee4a2c3_0): metadata   [feature server](https://gisdata.dot.ca.gov/arcgis/rest/services/CHrailroad/CA_HQ_Transit_Areas/FeatureServer) or [map server](https://gisdata.dot.ca.gov/arcgis/rest/services/CHrailroad/CA_HQ_Transit_Areas/MapServer)
1. [HQTA Stops](https://gis.data.ca.gov/datasets/f6c30480f0e84be699383192c099a6a4_0): metadata [feature server](https://gisdata.dot.ca.gov/arcgis/rest/services/CHrailroad/CA_HQ_Transit_Stops/FeatureServer) or [map server](https://gisdata.dot.ca.gov/arcgis/rest/services/CHrailroad/CA_HQ_Transit_Stops/MapServer)
1. [CA Transit Routes](https://gis.data.ca.gov/datasets/dd7cb74665a14859a59b8c31d3bc5a3e_0): metadata [feature server](https://gisdata.dot.ca.gov/arcgis/rest/services/CHrailroad/CA_Transit_Routes/FeatureServer) or [map server](https://gisdata.dot.ca.gov/arcgis/rest/services/CHrailroad/CA_Transit_Routes/MapServer)
1. [CA Transit Stops](https://gis.data.ca.gov/datasets/900992cc94ab49dbbb906d8f147c2a72_0): metadata [feature server](https://gisdata.dot.ca.gov/arcgis/rest/services/CHrailroad/CA_Transit_Stops/FeatureServer) or [map server](https://gisdata.dot.ca.gov/arcgis/rest/services/CHrailroad/CA_Transit_Stops/MapServer)
1. All GTFS datasets [metadata/data dictionary](https://data.ca.gov/dataset/cal-itp-gtfs-ingest-pipeline-dataset/resource/e26bf6ee-419d-4a95-8e4c-e2b13d5de793)

## GTFS Schedule Routes & Stops Geospatial Data

Traffic Ops had a request for all transit routes and transit stops to be published in the open data portal. 

1. Update `update_vars.py` for current month
1. In terminal: `make create_gtfs_schedule_geospatial_open_data`
   * [prep_traffic_ops](./prep_traffic_ops.py): helper functions for creating `routes` and `stops` datasets
   * [create_routes_data](./create_routes_data.py): functions to assemble routes that appear in `shapes`
   * [create_stops_data](./create_stops_data.py): functions to assemble stop data

[![stops_routes_mermaid](https://mermaid.ink/img/pako:eNqFkM0KwjAQhF8l7Ll5gQgexKsnPQbKkmxtoPkh2SBS-u6mVbxV9zQM3wzszGCiJVAgpdSBHU-kREwUhEVGHTZ7mOLDjJhZ3E4HHUQ7zi4VIeVRGOw5YyiO-xwrU_kQZcREO8iPjsIxfStW_Q_o2XnaoaADT9mjs-3Dec1o4JE8aVBNWhqwTqxBh6WhWDlen8GA4lypg5raBnR2eM_oQQ04leaSdRzz5b3aNt7yAjdubxo?type=png)](https://mermaid.live/edit#pako:eNqFkM0KwjAQhF8l7Ll5gQgexKsnPQbKkmxtoPkh2SBS-u6mVbxV9zQM3wzszGCiJVAgpdSBHU-kREwUhEVGHTZ7mOLDjJhZ3E4HHUQ7zi4VIeVRGOw5YyiO-xwrU_kQZcREO8iPjsIxfStW_Q_o2XnaoaADT9mjs-3Dec1o4JE8aVBNWhqwTqxBh6WhWDlen8GA4lypg5raBnR2eM_oQQ04leaSdRzz5b3aNt7yAjdubxo)

## Metadata Automation Steps and References
1. Add your dataset to `catalog.yml` and run `gcs_to_esri`.
    * In terminal: `cd open_data` followed by `python gcs_to_esri.py` 
    * The log will show basics like column names and EPSG. Make sure the metadata reflects the same info!
    * Only use EPSG:4326 (WGS84). All open data portal datasets will be in WGS84.
    * Download the zipped shapefiles from the Hub to your local filesystem.
1. If there are new datasets to add or changes to make, make them in `metadata.yml` and/or `data_dictionary.yml`. 
   * If there are changes to make in `metadata.yml`, make them. Afterwards, in terminal, run: `python supplement_meta.py`
1. Run [arcgis_pro_script](./arcgis_pro_script.py) to create XML files.
    * Open a notebook in Hub and find the `ARCGIS_PATH`
    * Hardcode that path for `arcpy.env.workspace = ARCGIS_PATH`
    * The exported XML metadata will be in file gdb directory.
    * Upload the XML metadata into Hub in `open_data/xml/`.
1. If there are new datasets added, open `open_data.py` and modify the script.
1. In terminal: `python open_data.py`.
    * Change into the `open_data` directory: `cd open_data/`.
    * The overwritten XML is stored in `open_data/metadata_xml/run_in_esri/`.
    * Download the overwritten XML files locally to run in ArcGIS.
1. Run [arcgis_pro_script](./arcgis_pro_script.py) after import the updated XML metadata for each feature class.
   * There are steps to create FGDC templates for each datasets to store field information.
   * This only needs to be done once when a new dataset is created. 
1. In terminal: `python cleanup.py` to clean up old XML files and remove zipped shapefiles.

### Metadata
* [Metadata](./metadata.yml)
* [Data dictionary](./data_dictionary.yml)
* [update_vars](./update_vars.py) and [publish_utils](./publish_utils.py) contain a lot of the variables that would frequently get updated in the publishing process.
   * Apply standardized column names across published datasets, even they differ from internal keys (`org_id` in favor of `gtfs_dataset_key`, `agency` in favor of `organization_name`). 
   * Since we do not save multiple versions of published datasets, the columns are renamed prior to exporting the geoparquet as a zipped shapefile.

## Open Data Intake Process 
* Open a [ticket](https://forms.office.com/Pages/ResponsePage.aspx?id=ZAobYkAXzEONiEVA00h1VuRQZHWRcbdNm496kj4opnZUNUo1NjRNRFpIOVRBMVFFTFJDM1JKNkY0SC4u) on the Intranet to update or add new services and provide [justification](./intake_justification.md)