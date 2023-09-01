import os
import arcpy
import json

from arcpy import metadata as md

arcpy.env.workspace = os.path.join(
    "C:\\", "Users", "s153936", 
    "Documents", "ArcGIS"
)
working_dir = arcpy.env.workspace

directory = arcpy.GetInstallInfo("desktop")["InstallDir"] 

# Set datasets to update
in_features = [
    'ca_hq_transit_areas',
    'ca_hq_transit_stops',
    'ca_transit_routes',
    'ca_transit_stops',
    'speeds_by_stop_segments',
    'speeds_by_route_time_of_day'
]

staging_location = 'staging.gdb'
out_location = 'open_data.gdb'
#schema_location = 'staging_schema.gdb'


def feature_class_in_gdb_path(my_gdb, file_name):
    return os.path.join(my_gdb, file_name)


# Clean up last run (if applicable)
for f in in_features:
    feature_path = f"{working_dir}\{f}.xml"
    if os.path.exists(feature_path):
        os.remove(feature_path)

        
## (1) Convert shapefile layer to gdb feature class
for f in in_features:
    
    # construct the filename, which is takes form of routes_assembled/routes_assembled.shp
    shp_file_name = f"{os.path.join(f, f'{f}.shp')}"

    
    this_feature_class = os.path.join(staging_location, f)
    
    if arcpy.Exists(this_feature_class): 
        arcpy.management.Delete(this_feature_class)

    # Execute FeatureClassToGeodatabase
    arcpy.FeatureClassToGeodatabase_conversion(
        shp_file_name, 
        staging_location
    )
    
    # Print field names, just in case it needs renaming
    # get a list of fields for each feature class
    field_list = arcpy.ListFields(this_feature_class)  
    
    print(this_feature_class)
    
    for field in field_list: 
        print(field.name)


## (2) Read in json with all the changes we need for each layer
with open(f"{working_dir}\metadata.json") as f:
    meta_dict = json.load(f)

    
def rename_columns_with_dict(this_feature_class, rename_dict: dict):
    """
    Get a list of fields for each feature class and use a dict to rename.
    """
    field_list = arcpy.ListFields(this_feature_class)  

    for field in field_list: 
        if field.name in rename_dict: 
            arcpy.AlterField_management(
                this_feature_class, 
                field.name, 
                rename_dict[field.name], # new_field_name
                rename_dict[field.name] # new_field_alias
            ) 
    return


def update_layer_with_json(feature_class_list: list, meta_dict_supplied: dict):
    """
    Update each feature layer.
    Rename columns, update the metadata class attributes
    that can be accessed through the arcpy.metadata class.
    """
    for f in feature_class_list:
        # To change field names, must use AlterField_management, 
        # because changing it in XML won't carry through when you sync
        this_feature_class = feature_class_in_gdb_path(staging_location, f)
        
        subset_meta_dict = meta_dict_supplied[f]
        
        if "rename_cols" in subset_meta_dict.keys():  
            rename_dict = subset_meta_dict["rename_cols"]
            
            rename_columns_with_dict(this_feature_class, rename_dict)
        
        
        # Check that renaming is done
        print(this_feature_class)
        check_fields = arcpy.ListFields(this_feature_class)
        for field in check_fields:
            print(field.name)
        
        # Now update metadata class elements that are available
        source_metadata = md.Metadata(this_feature_class)
        source_metadata.title = subset_meta_dict["dataset_name"]
        source_metadata.tags = subset_meta_dict["theme_keywords"]
        source_metadata.summary = subset_meta_dict["summary_purpose"]
        source_metadata.description = subset_meta_dict["description"]
        source_metadata.accessConstraints = subset_meta_dict["public_access"]
        source_metadata.save()
        
    return


update_layer_with_json(in_features, meta_dict)

    
## (3) Export metadata associated with file gdb feature class in ISO19139 format    
for f in in_features:
    this_feature_class = feature_class_in_gdb_path(staging_location, f)

    # Original metadata
    # Migrating to Pro: https://pro.arcgis.com/en/pro-app/latest/arcpy/metadata/migrating-from-arcmap-to-arcgis-pro.htm

    source_metadata = md.Metadata(this_feature_class)
    # Export metadata XML    
    meta_output = os.path.join(working_dir, f"{f}.xml")
            
    # In ArcGIS Pro, instead of FGDC for Desktop, use ISO 19139 GML 3.2
    # https://sv03tmcpo.ct.dot.ca.gov/portal/apps/sites/#/geep/pages/open-data-request
    TRANSLATOR = "ISO19139_GML32"     
    
    source_metadata.exportMetadata(
        outputPath = meta_output, 
        metadata_export_option = TRANSLATOR
    )
    
    print(f"successful export: {f}")


### (4) UPDATE XML METADATA SEPARATELY IN PYTHON OUTSIDE OF ARCGIS IN JUPYTERHUB
# Once updated XML is back in ArcPro, synchronize it with existing layer
def apply_updated_xml(feature_class):
    """
    Synchronize new XML with existing feature class.
    """
    this_feature_class = feature_class_in_gdb_path(
        staging_location, 
        feature_class
    )
    
    source_metadata = md.Metadata(this_feature_class)
    source_metadata.synchronize(metadata_sync_option="ALWAYS")
    source_metadata.save()
    
    return


for f in in_features:
    apply_updated_xml(f)

    
## (5) Copy the feature class from staging location to out location
# Use staging location and out location because otherwise, arcpy errors when it detects
# another XML when you try and update the layer in a subsequent update
for f in in_features:
    # Delete the feature class in this gdb, because we don't want _1 appended to end
    staging_feature_class = feature_class_in_gdb_path(staging_location, f)
    out_feature_class = feature_class_in_gdb_path(out_location, f)

    if arcpy.Exists(out_feature_class): 
        arcpy.management.Delete(out_feature_class)

    # Copy over the feature class from staging.gdb to open_data.gdb
    # Since we already manually imported XML in staging, 
    # when this feature class is moved to out_location, it takes the new XML with it
    arcpy.conversion.FeatureClassToFeatureClass(
        staging_feature_class, 
        out_location, 
        f
    )


## (7) Move from file gdb to enterprise gdb
# License Select must be set to Advanced for this to work
# Exit and restart ArcPro to clear locks on layers in overwriting
# If we don't exit, the layer will be locked because it shows we're already using it 
# staging to open_data), and it will prevent writing from open_data to the enterprise gdb.

ENTERPRISE_DATABASE = "Database Connections/HQrail(edit)@sv03tmcsqlprd1.sde"

for f in in_features:
    out_feature_class = feature_class_in_gdb_path(out_location, f)
    
    arcpy.FeatureClassToFeatureClass_conversion(
        in_features = out_feature_class,
        out_path = ENTERPRISE_DATABASE,
        out_name = f
    )