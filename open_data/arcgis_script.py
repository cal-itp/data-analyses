"""
Script to run in ArcGIS.
More limited because it relies on `arcpy`
"""
## Run this in Hub -- dotenv cannot be run in ArcGIS
# Set this path to be in _env
# Hardcode it in the script within ArcGIS
import dotenv
import os
dotenv.load_dotenv("_env")
ARCGIS_PATH = os.environ["ARCGIS_PATH"]


# Save a version of script that runs within ArcGIS
import arcpy
import os

#arcpy.env.workspace = "C:\Users\s153936\Documents\ArcGIS"
arcpy.env.workspace = ARCGIS_PATH
ENTERPRISE_PATH  = 'C:\Users\s153936\Documents\ArcGIS\AppData\Roaming\ESRI\Desktop10.8\ArcCatalog\HQrail(edit)@sv03tmcsqlprd1.sde'


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

# Export metadata using FGDC
translator =  directory + 'Metadata\Translator\ArcGIS2FGDC.xml'

## (1) Convert shapefile layer to gdb feature class
for f in in_features:
    # construct the filename, which is takes form of routes_assembled/routes_assembled.shp
    shp_file_name = f + '/' + f + '.shp'
    
    # Need this try/except because arcpy won't let you overwrite, 
    # and will error if it finds it 
    try:
        arcpy.management.Delete(staging_location + '/' + f)
    except:
        pass

    # Execute FeatureClassToGeodatabase
    arcpy.FeatureClassToGeodatabase_conversion(shp_file_name, staging_location)
    
    # Print field names, just in case it needs renaming
    field_list = arcpy.ListFields(
        staging_location + '/' + f)  #get a list of fields for each feature class
    
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
    #hqta.RENAME_CA_HQTA
    RENAME_CA_HQTA = {
        "calitp_itp": "itp_id_primary",
        "agency_nam": "agency_primary",
        "calitp_i_1": "itp_id_secondary",
        "agency_n_1": "agency_secondary",
        "calitp_id_": "itp_id_primary",
        "agency_pri": "agency_primary",
        "calitp_i_1": "itp_id_secondary",
        "agency_sec": "agency_secondary",
        "hqta_detai": "hqta_details",
    }
    
    # To change field names, must use AlterField_management, 
    # because changing it in XML won't carry through when you sync
    field_list = arcpy.ListFields(
        staging_location + '/' + f)  #get a list of fields for each feature class

    for field in field_list: #loop through each field
        if field.name in RENAME_CA_HQTA:  #look for the name elev
            arcpy.AlterField_management(
                staging_location + '/' + f, 
                field.name, RENAME_CA_HQTA[field.name])
    
    
## (3) Export metadata associated with file gdb feature class in FGDC format    
for f in in_features:
    # Construct XML filename
    # Spit out one that's before it's manually changed
    xml_file = staging_location + '/' + f + '.xml'
    # Export metadata XML
    arcpy.ExportMetadata_conversion(staging_location + '/' + f, 
                                     translator, 
                                     xml_file)
    


### (4) UPDATE XML METADATA SEPARATELY IN PYTHON OUTSIDE OF ARCGIS

## (5) Copy the feature class from staging location to out location
# In the out location, we can drop the new XML and use it to sync
# Use staging location and out location because otherwise, arcpy errors when it detects
# another XML when you try and update the layer in a subsequent update
for f in in_features:
    # Delete the feature class in this gdb, because we don't want _1 appended to end
    try:
        arcpy.management.Delete(out_location + '/' + f)
    except:
        pass
    
    # Copy over the feature class from staging.gdb to open_data.gdb
    arcpy.conversion.FeatureClassToFeatureClass(staging_location + '/' + f, 
                                                out_location + '/', 
                                                f)

## (6) Sync the XML with the feature class    
for f in in_features:
    # This is the one after it's manually changed. Keep separate to see what works.
    updated_xml_file = out_location + '/' + f + '.xml'

    # Import the updated xml, then overwrite the metadata in the file gdb    
    arcpy.conversion.ImportMetadata(updated_xml_file, 
                                    "FROM_FGDC", 
                                    out_location + '/' + f, 
                                    "ENABLED")
    
    
## (7) Clean up XML file in staging.gdb
# If not, next time, it will error because it can't output an XML file 
# when one is present (no overwriting)
for f in in_features:
    try:
        os.remove(xml_file)
    except:
        pass

## (8) Move from file gdb to enterprise gdb?
