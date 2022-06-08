# Automation Steps and References

1. Dataset created in Hub. Save as shapefile. Do we version one as geoparquet in GCS?
2. Shapefile written as feature class in file gdb
* Start with `arcgis_script`
* [Edit metadata for many ArcGIS items](https://desktop.arcgis.com/en/arcmap/latest/manage-data/metadata/editing-metadata-for-many-arcgis-items.htm)
* [Metadata template -- is this needed?](https://desktop.arcgis.com/en/arcmap/latest/manage-data/metadata/creating-a-metadata-template.htm)
* [Convert shp to gdb](https://gis.stackexchange.com/questions/269701/copying-multiple-shp-files-to-a-file-geodatabase)
* [export features to gdb](https://gis.stackexchange.com/questions/366054/export-features-to-geodatabase-created-in-same-python-script)
3. Export metadata associated with feature class as XML
4. [Convert XML to JSON using xmltodict](https://stackoverflow.com/questions/48821725/xml-parsers-expat-expaterror-not-well-formed-invalid-token)
5. Supply a dictionary with the relevant info (`parse-metadata`)
6. Overwrite values with dictionary in the JSON (`parse-metadata`)
7. [Convert JSON back to XML](https://gis.stackexchange.com/questions/202978/converting-xml-dict-xml-using-python) and overwrite XML
8. Overwrite the XML and import this, overwrite/sync this metadata with feature class
9. Zip the file gdb