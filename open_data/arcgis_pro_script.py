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
                field.name, RENAME_CA_HQTA[field.name])\


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

