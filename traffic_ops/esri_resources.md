## Packages

### Python only
* [pyshp](https://github.com/GeospatialPython/pyshp)
    * [took out editor, can't overwrite metadata](https://github.com/GeospatialPython/pyshp/issues/77)
    * [Use older version to get editor class](https://github.com/GeospatialPython/pyshp/blob/master/changelog.txt) `!pip install pyshp==1.2.12`

### ESRI
* [arcpy_metadata](https://github.com/ucd-cws/arcpy_metadata)


## Shapefile to Geodatabase
* [Convert shp to gdb](https://gis.stackexchange.com/questions/269701/copying-multiple-shp-files-to-a-file-geodatabase)
* [export features to gdb](https://gis.stackexchange.com/questions/366054/export-features-to-geodatabase-created-in-same-python-script)


## Set up XML

## Edit XML
* [Edit XML with Python](https://stackoverflow.com/questions/23013236/how-to-encode-xml-into-esri-shapefiles-using-python)
* [Find attribute value in metadata](https://gis.stackexchange.com/questions/115833/getting-value-from-metadata-in-python-script-for-attribute)
* [Update attributes with pyshp](https://gis.stackexchange.com/questions/57635/updating-attributes-using-pyshp)
* [Encode XML into shapefile](https://stackoverflow.com/questions/23013236/how-to-encode-xml-into-esri-shapefiles-using-python). Requires pyshp: https://pypi.python.org/pypi/pyshp

## Tried but probably irrelevant

* `pyshp`: https://github.com/GeospatialPython/pyshp
* `pyshp` doesn't appear to support writing metadata: https://github.com/GeospatialPython/pyshp/issues/77, https://gis.stackexchange.com/questions/57635/updating-attributes-using-pyshp
* `arcpy_metadata`: https://github.com/ucd-cws/arcpy_metadata
* Metadata stored as XML in shapefile folder.
https://desktop.arcgis.com/en/arcmap/10.3/manage-data/metadata/the-arcgis-metadata-format.htm
* https://chrishavlin.com/tag/pyshp/. Sample XML: https://water.usgs.gov/GIS/metadata/usgswrd/XML/physio.xml, https://github.com/chrishavlin/learning_shapefiles/blob/master/src/inspect_shapefile.py
* `!pip install pyshp` -- works, but can't deal with metadata
* `!pip install arcpy_metadata` -- we don't have arcpy
* `!pip install dbfpy` -- works, but then the module won't import
* `import dbfpy` works. but `dbfpy.dbf` doesn't work
* https://github.com/GeospatialPython/pyshp/blob/master/changelog.txt. Use a much older version to get the Editor class: `!pip install pyshp==1.2.12`

## ArcGIS
* [Metadata template](https://desktop.arcgis.com/en/arcmap/latest/manage-data/metadata/creating-a-metadata-template.htm)
* [Metadata workflow editing multiple items](https://desktop.arcgis.com/en/arcmap/latest/manage-data/metadata/editing-metadata-for-many-arcgis-items.htm)
* [Loading Python script](https://desktop.arcgis.com/en/arcmap/10.3/analyze/executing-tools/saving-loading-and-recalling-at-the-python-window.htm)
* 


## Other
* [Convert XML to shp](https://gis.stackexchange.com/questions/71182/programmatically-converting-arbitrary-xml-data-to-shapefile)

