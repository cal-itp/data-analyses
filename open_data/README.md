# README

## Open Data Portal
1. [HQTA Areas](https://gis.data.ca.gov/datasets/863e61eacbf3463ab239beb3cee4a2c3_0): metadata   [feature server](https://gisdata.dot.ca.gov/arcgis/rest/services/CHrailroad/CA_HQ_Transit_Areas/FeatureServer) or [map server](https://gisdata.dot.ca.gov/arcgis/rest/services/CHrailroad/CA_HQ_Transit_Areas/MapServer)
1. [HQTA Stops](https://gis.data.ca.gov/datasets/f6c30480f0e84be699383192c099a6a4_0): metadata [feature server](https://gisdata.dot.ca.gov/arcgis/rest/services/CHrailroad/CA_HQ_Transit_Stops/FeatureServer) or [map server](https://gisdata.dot.ca.gov/arcgis/rest/services/CHrailroad/CA_HQ_Transit_Stops/MapServer)
1. [CA Transit Routes](https://gis.data.ca.gov/datasets/dd7cb74665a14859a59b8c31d3bc5a3e_0): metadata [feature server](https://gisdata.dot.ca.gov/arcgis/rest/services/CHrailroad/CA_Transit_Routes/FeatureServer) or [map server](https://gisdata.dot.ca.gov/arcgis/rest/services/CHrailroad/CA_Transit_Routes/MapServer)
1. [CA Transit Stops](https://gis.data.ca.gov/datasets/900992cc94ab49dbbb906d8f147c2a72_0): metadata [feature server](https://gisdata.dot.ca.gov/arcgis/rest/services/CHrailroad/CA_Transit_Stops/FeatureServer) or [map server](https://gisdata.dot.ca.gov/arcgis/rest/services/CHrailroad/CA_Transit_Stops/MapServer)
1. [CA Average Transit Speeds by Stop-to-Stop Segments](https://gis.data.ca.gov/datasets/4937eeb59fdb4e56ae75e64688c7f2c0_0/): metadata [feature server](https://caltrans-gis.dot.ca.gov/arcgis/rest/services/CHrailroad/Speeds_by_Stop_Segments/FeatureServer/0) or [map server](https://caltrans-gis.dot.ca.gov/arcgis/rest/services/CHrailroad/Speeds_by_Stop_Segments/MapServer/0)
1. [CA Average Transit Speeds by Route and Time of Day](https://gis.data.ca.gov/datasets/071df783099f4224b7ebb54839eae007_0/): metadata [feature server](https://caltrans-gis.dot.ca.gov/arcgis/rest/services/CHrailroad/Speeds_by_Route_Time_of_Day/FeatureServer/0) or [map server](https://caltrans-gis.dot.ca.gov/arcgis/rest/services/CHrailroad/Speeds_by_Route_Time_of_Day/MapServer/0)
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
1. If this is the first time running, do the following:
    * Create a new ArcGIS Pro project. In its home folder, place the HQRail .sde file, which you will need provided to you.
    * Download and install [Microsoft SQL Server](https://learn.microsoft.com/en-us/ssms/install/install?view=sql-server-2017). This will require assistance from IT.
1. Add all datasets that are to be pulished to the `RUN_ME` list in `update_vars.py`.
1. Add all datasets that are to be published to `catalog.yml` and run `gcs_to_esri`.
    * In terminal: `cd open_data` followed by `python gcs_to_esri.py`
    * Since this file is under source control, it only needs to be changed if new datasets are being added.
    * The log will show basics like column names and EPSG. Make sure the metadata reflects the same info!
    * Only use EPSG:4326 (WGS84). All open data portal datasets will be in WGS84.
1. If there are new datasets to add or changes to make, make them in `metadata.yml` and/or `data_dictionary.yml`.
   * If there are changes to make in `metadata.yml`, make them. Afterwards, in terminal, run: `python supplement_meta.py`
1. If there are changes to be made to metadata.yml (adding new datasets, changing descriptions, change contact information, etc), make them. This is infrequent. An updated analysis date is already automated and does not have to be updated here.
1. In the JupyterHub terminal: `python supplement_meta.py`
1. In the JupyterHub terminal: `python update_data_dict.py`.
   * Check the log results, which tells you if there are columns missing from `data_dictionary.yml`. These columns and their descriptions need to be added. Every column in the ESRI layer must have a definition, and where there's an external data dictionary website to cite, provide a definition source.
1. In the JupyterHub terminal: `python update_fields_fgdc.py`. This populates fields with `data_dictionary.yml` values.
    * Only run if `update_data_dict` had changes to incorporate
1. Download all zipped shapefiles created by `gcs_to_esri.py` and `metadata.json` to your ArcGIS project directory. Delete any existing xml files in this directory that may have been generated in prior runs.
1. Open ArcGIS Pro to the project with the .sde file.
1. In ArcGIS, run the ArcGIS Notebook to create XML files.
    * Download `arcgis_pro_script.py` and `arcgis_pro_notebook_sample.ipynb` to your ArcGIS project.
    * Open the notebook in ArcGIS and update the value of `arcpy.env.workspace` to match the path to your ArcGIS working directory. This is likely the directory created as your ArcGIS "project", and should contain your downloaded files.
    * Run the notebook up to the markdown instructions to "Do field data dictionary updates in Jupyter Hub".
    * Check that two xml files have been updated per dataset, one should have have the same name of the dataset and one should be suffixed `_fgdc`.
1. Upload the non-fgdc XML files to JupyterHub at `open_data/xml/`.
1. In the JupyterHub terminal: `python metadata_update_pro.py`.
    * The overwritten XML is stored in `open_data/xml/run_in_esri/`.
    * Download the overwritten XML files locally to run in ArcGIS.
1. For each dataset, update the FGDC XML:
    * From the ribbon, select "View", then "Catalog view" (not to be confused with the Catalog pane)
    * From the tab that opens, open "Folders", then your project's working directory, then open `staging.gdb`
    * You should see a list of each dataset. For each dataset, select it (click once), then from the ribbon select "Catalog" then Import in the metadata section. There will likely be several identically labeled buttons that say "Import", but only the one within the metadata section is useful here.
    * You should see a popup window with two display boxes - one that says "Import metadata from", and one that says "The type of metadata to import". In "Import metadata from" box, enter the path to the `*_fgdc.xml` file that was generated earlier. You should be able to open a file browser with the little folder icon. In the "The type of metadata to import" box, select the option containing "FGDC".
    * Repeat this process for each dataset.
1. Grant yourself "Advanced" priviliges for ArcGIS:
   * Save everything, and close ArcGIS Pro entirely
   * Open "ArcGISLicenseSelect" (notice no spaces) from the Start menu
   * Set your "ArcGIS Pro" license versio to "Advanced"
   * Re-open your project in ArcGIS Pro
1. Delete any existing databases that you intend to overwrite on the HQRail Enterprise Geodatabase (the .sde file).
1. Run the remainder of the ArcGIS [notebook](./arcgis_pro_notebook_sample.ipynb) after importing the updated XML metadata for each feature class.
   * Note that since you re-started ArcGIS, the notebook's kernel will restart and you will need to re-run the first few cells.
   * Once this completes, the new data will be added to the HQRail enterprise geodatabase, and you should be able to add datasets from there to the map.
   * There are steps to create FGDC templates for each datasets to store field information, but this only needs to be done once when a new dataset is created.
1. In terminal: `python cleanup.py` to clean up old XML files and remove zipped shapefiles.
   * The YAML and XML files created/have changes get checked into GitHub.

### Metadata
* [Metadata](./metadata.yml)
* [Data dictionary](./data_dictionary.yml)
* [update_vars](./update_vars.py) contains a lot of the variables that would frequently get updated in the publishing process.
   * Apply standardized column names across published datasets, even they differ from internal keys (`org_id` in favor of `gtfs_dataset_key`, `agency` in favor of `organization_name`).
   * Since we do not save multiple versions of published datasets, the columns are renamed prior to exporting the geoparquet as a zipped shapefile.

## Open Data Intake Process
* Do a final check of datasets uploaded to the enterprise geodatabase (column names? metadata? record counts?).
   * Check that column names match your expectations
   * Check that metadata is correct.
       * To view all metadata, you may need to select "Project" from the ArcGIS ribbon, then "Options", then "Metadata", then from the dropdown labeled "Metadata style" select "FGDC CSDGM Metadata"
       * The CRS should be EPSG:4326, and all columns should have a description
   * Check that the geographic extent of each layer seems plausible
   * Check that the number of records is comparable to the number of records for the prior month.
* Open a [ticket](https://d3giscoreage.dot.ca.gov/portal/apps/sites/#/geep/pages/dp-open-data) on the Intranet to update or add new services and provide [justification](./intake_justification.md)
