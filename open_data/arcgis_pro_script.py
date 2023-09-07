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


def feature_class_in_gdb_path(my_gdb, file_name):
    return os.path.join(my_gdb, file_name)


## Set FGDC field defs for each dataset and export XML (do once when new dataset added)
# Only the FGDC standard keeps fields. 
# See if we can use this and combine it with our ISO 19139 standard later.

def export_fgdc_metadata(one_feature_class):
    """
    Export XML as FGDC format, 
    that's the only one that keeps field names and definitions
    available.
    """
    this_feature_class = feature_class_in_gdb_path(
        staging_location, 
        one_feature_class
    )
    
    source_metadata = md.Metadata(this_feature_class)
    
    # Export metadata XML in FGDC   
    meta_output = os.path.join(working_dir, 
                               f"{one_feature_class}_fgdc.xml")
            
    TRANSLATOR = "FGDC_CSDGM"     

    source_metadata.exportMetadata(
        outputPath = meta_output, 
        metadata_export_option = TRANSLATOR
    )
    print(f"Exported FGDC XML for {one_feature_class}")
    
    
'''
for f in in_features:
    export_fgdc_metadata(f)
'''

## Do field data dictionary updates in Jupyter Hub

## Use shapefile and write it to a file gdb layer¶

# Clean up last run (if applicable)
for f in in_features:
    feature_path = f"{working_dir}\{f}.xml"
    if os.path.exists(feature_path):
        os.remove(feature_path)

        
## (1) Convert shapefile layer to gdb feature class
def shp_to_feature_class(file_name: str):
    """
    From shapefile (directory of files), unpack those
    and write it to our staging gdb as a feature class.
    """
    # construct the filename, which is takes form of routes_assembled/routes_assembled.shp
    shp_file_name = f"{os.path.join(file_name, f'{file_name}.shp')}"
    
    this_feature_class = os.path.join(staging_location, file_name)
    
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
    
    return


for f in in_features:
    shp_to_feature_class(f)


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


def import_fgdc_metadata_and_sync(one_feature_class):
    """
    Export XML as FGDC format, 
    that's the only one that keeps field names and definitions
    available.
    Do this separately becuase it operates on the feature class name, 
    and the xml file sits outside of a file gdb.
    """
    this_feature_class = feature_class_in_gdb_path(
        staging_location, 
        one_feature_class
    )
    
    # Open the staging dataset's metadata
    source_metadata = md.Metadata(this_feature_class)  
    
    # Synchronize it with the field info populated from the FGDC template 
    source_metadata.importMetadata(f"{one_feature_class}_fgdc.xml")
    source_metadata.save()
    
    return


def update_metadata_class(this_feature_class, meta_dict_for_dataset: dict):
    """
    Update the elements in the arcpy.metadata class.
    """
    # Now update metadata class elements that are available
    source_metadata = md.Metadata(this_feature_class)

    source_metadata.title = meta_dict_for_dataset["dataset_name"]
    source_metadata.tags = meta_dict_for_dataset["theme_keywords"]
    source_metadata.summary = meta_dict_for_dataset["summary_purpose"]
    source_metadata.description = meta_dict_for_dataset["description"]
    source_metadata.accessConstraints = meta_dict_for_dataset["public_access"]
    source_metadata.save()
    
    return


def update_feature_class_with_json(one_feature_class, meta_json_dict: dict):
    """
    Update a single feature class.
    Rename columns, apply FGDC metadata fields 
    template, and update metadata class attributes
    that can be accessed through the arcpy.metadata class.
    """
    this_feature_class = feature_class_in_gdb_path(
        staging_location, 
        one_feature_class
    )
        
    subset_meta_dict = meta_json_dict[one_feature_class]
        
    if "rename_cols" in subset_meta_dict.keys():  
        rename_dict = subset_meta_dict["rename_cols"]

        rename_columns_with_dict(this_feature_class, rename_dict)
    
    # Check that renaming is done
    print(this_feature_class)
    check_fields = arcpy.ListFields(this_feature_class)
    for field in check_fields:
        print(field.name)
    
    # Sync with FGDC metadata 
    # (this is on the one_feature_class, which sits outside of staging/)
    import_fgdc_metadata_and_sync(one_feature_class)
    
    # Now update the rest of the metadata elements
    update_metadata_class(this_feature_class, subset_meta_dict)

    return

    
for f in in_features:
    update_feature_class_with_json(f, meta_dict)

    
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

## Import FGDC metadata for each dataset manually
# The button to Metadata > Import > type of metadata set to FGDC does something different than the `metadata.importMetadata` feature, which doesn't do it. Manually doing the import for the fgdb metadata works for each dataset only.

## Write layers to open_data gdb
    
## (5) Copy the feature class from staging location to out location
# Use staging location and out location because otherwise, arcpy errors when it detects
# another XML when you try and update the layer in a subsequent update
# Write layers to open_data (with the overwritten and updated XML already)
def write_feature_class_to_open_data(
    one_feature_class,
    staging_gdb = staging_location, 
    output_gdb = out_location, 
):
    """
    Move the feature class from the staging gdb to the output gdb.
    Delete the feature class in the output gdb because
    we don't want _1 appended to the end
    """
    staging_feature_class = feature_class_in_gdb_path(
        staging_gdb, 
        one_feature_class
    )
    out_feature_class = feature_class_in_gdb_path(
        output_gdb, 
        one_feature_class
    )
    
    if arcpy.Exists(out_feature_class): 
        arcpy.management.Delete(out_feature_class)

    # Copy over the feature class from staging.gdb to open_data.gdb
    arcpy.conversion.FeatureClassToFeatureClass(
        staging_feature_class, 
        output_gdb, 
        one_feature_class
    )
    
    arcpy.conversion.FeatureClassToFeatureClass(
        staging_feature_class, 
        output_gdb, 
        one_feature_class
    )
    
    return
    

for f in in_features:
    write_feature_class_to_open_data(f)
    print(f"in open_data.gdb: {f}")


    
## Exit and restart ArcPro to clear locks on layers in overwriting
# If we don't exit, the layer will be locked because it shows we're already using it (staging to open_data), and it will prevent writing from open_data to the enterprise gdb.
# License Select must be set to `Advanced` for this to work

## (7) Move from file gdb to enterprise gdb
ENTERPRISE_DATABASE = "Database Connections/HQrail(edit)@sv03tmcsqlprd1.sde"

for f in in_features:
    out_feature_class = feature_class_in_gdb_path(out_location, f)
    
    arcpy.FeatureClassToFeatureClass_conversion(
        in_features = out_feature_class,
        out_path = ENTERPRISE_DATABASE,
        out_name = f
    )