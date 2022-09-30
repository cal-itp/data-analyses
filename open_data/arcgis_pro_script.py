import os
import arcpy

from arcpy import metadata as md

arcpy.env.workspace = os.path.join("C:\\", "Users", "s153936", "Documents", "ArcGIS")
working_dir = arcpy.env.workspace

# Set local variables
in_features = [
    'ca_hq_transit_areas',
    'ca_hq_transit_stops',
    'ca_transit_routes',
    'ca_transit_stops',
]

staging_location = 'staging.gdb'
out_location = 'open_data.gdb'

# Path to Metadata stylesheet
directory = arcpy.GetInstallInfo("desktop")["InstallDir"] 

def feature_class_in_gdb_path(my_gdb, file_name):
    return os.path.join(my_gdb, file_name)


## (1) Convert shapefile layer to gdb feature class
for f in in_features:
    # construct the filename, which is takes form of routes_assembled/routes_assembled.shp
    shp_file_name = f"{os.path.join(f, f'{f}.shp')}"
    
    this_feature_class = feature_class_in_gdb_path(staging_location, f)
    if arcpy.Exists(this_feature_class): 
        arcpy.management.Delete(this_feature_class)

    # Execute FeatureClassToGeodatabase
    arcpy.FeatureClassToGeodatabase_conversion(shp_file_name, staging_location)
    
    # Print field names, just in case it needs renaming
    field_list = arcpy.ListFields(
        os.path.join(staging_location, f))  #get a list of fields for each feature class
    
    for field in field_list: #loop through each field
        print(field.name)

## (2) Rename fields where needed
# Do this once it's a feature class, so we can preserve the new column names
# before metadata is created
need_renaming = [
    'ca_hq_transit_areas',
    'ca_hq_transit_stops',
]


for f in need_renaming:
    # Grab this renaming dict
    #hqta.RENAME_CA_HQTA (UPDATE THESE, FEWER NOW)
    RENAME_CA_HQTA = {
        "itp_id_pri": "itp_id_primary",
        "agency_pri": "agency_primary",
        "itp_id_sec": "itp_id_secondary",
        "agency_sec": "agency_secondary",
        "hqta_detai": "hqta_details",
    }

    # To change field names, must use AlterField_management, 
    # because changing it in XML won't carry through when you sync
    this_feature_class = feature_class_in_gdb_path(staging_location, f)

    field_list = arcpy.ListFields(this_feature_class)  #get a list of fields for each feature class

    for field in field_list: #loop through each field
        if field.name in RENAME_CA_HQTA:  #look for the name elev
            arcpy.AlterField_management(
                this_feature_class, 
                field.name, RENAME_CA_HQTA[field.name], # new_field_name
                RENAME_CA_HQTA[field.name]) # new_field_alias
            
            
# Double check it's done
# TODO: this does look like it renames it...but when XML is exported in next step
# the new field names are not retained
for f in need_renaming:
    this_feature_class = os.path.join(staging_location, f)

    # Print field names, just in case it needs renaming
    field_list = arcpy.ListFields(this_feature_class)  #get a list of fields for each feature class
    
    print(this_feature_class)
    for field in field_list: #loop through each field
        print(field.name)


## (3) Export metadata associated with file gdb feature class in FGDC format    
for f in in_features:
    this_feature_class = feature_class_in_gdb_path(staging_location, f)

    # Original metadata
    # Migrating to Pro: https://pro.arcgis.com/en/pro-app/latest/arcpy/metadata/migrating-from-arcmap-to-arcgis-pro.htm

    source_metadata = md.Metadata(this_feature_class)

    # Export metadata XML    
    meta_output = os.path.join(working_dir, f"{f}.xml")
    TRANSLATOR = "FGDC_CSDGM" 
    
    source_metadata.exportMetadata(outputPath = meta_output, 
                               metadata_export_option=TRANSLATOR)
    
    print(f"successful export: {f}")


### (4) UPDATE XML METADATA SEPARATELY IN PYTHON OUTSIDE OF ARCGIS
## Note: now there are some parts, bounding boxes, etc that isn't
# present in XML. Just comment those parts out in metadata_update.py

## (5) Copy the feature class from staging location to out location
# In the out location, we can drop the new XML and use it to sync
# Use staging location and out location because otherwise, arcpy errors when it detects
# another XML when you try and update the layer in a subsequent update


## (6) Sync the XML with the feature class    
for f in in_features:
    # This is the one after it's manually changed. Keep separate to see what works.
    # There's already XML files for each feature class, so just copy and paste 
    # the new XML from metadata_xml/run_in_esri and replace those
    updated_xml_file = f"{working_dir}{f}.xml"

    out_feature_class = feature_class_in_gdb_path(out_location, f)
    
    # Import the updated xml, then overwrite the metadata in the file gdb 
    meta = md.Metadata(out_feature_class)

    meta.importMetadata(updated_xml_file, "FGDC_CSDGM")
    meta.save()

    
## (7) Old #7 is to clear the XML from staging...not needed, since 
# ArcGIS Pro can't write to staging or open data gdb, but has
# to write to working_dir

## (7) Move from file gdb to enterprise gdb
# License Select must be set to Advanced for this to work
ENTERPRISE_DATABASE = "Database Connections/HQrail(edit)@sv03tmcsqlprd1.sde"

for f in in_features:
    out_feature_class = feature_class_in_gdb_path(out_location, f)
    
    arcpy.FeatureClassToFeatureClass_conversion(
        in_features = out_feature_class,
        out_path = ENTERPRISE_DATABASE,
        out_name = f)