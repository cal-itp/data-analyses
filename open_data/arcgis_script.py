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
#translator =  directory + 'Metadata\Translator\ArcGIS2ISO19139.xml'

# Convert shapefile layer to gdb feature class
for f in in_features:
    # construct the filename, which is takes form of routes_assembled/routes_assembled.shp
    shp_file_name = f + '/' + f + '.shp'
    
    # Need this try/except because arcpy won't let you overwrite, and will error if it finds it 
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

        
# Rename fields
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
    
    
for f in in_features:
    # Construct XML filename
    # Spit out one that's before it's manually changed
    xml_file = staging_location + '/' + f + '.xml'
    # Export metadata XML
    arcpy.ExportMetadata_conversion(staging_location + '/' + f, 
                                     translator, 
                                     xml_file)
    


### UPDATE XML METADATA SEPARATELY IN PYTHON OUTSIDE OF ARCGIS

# Run this after putting the updated XML in the file gdb
# Clean up the open_data file gdb
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

    
for f in in_features:
    # This is the one after it's manually changed. Keep separate to see what works.
    updated_xml_file = out_location + '/' + f + '.xml'

    # Import the updated xml, then overwrite the metadata in the file gdb    
    arcpy.conversion.ImportMetadata(updated_xml_file, 
                                    "FROM_FGDC", 
                                    out_location + '/' + f, 
                                    "ENABLED")
    
    
# Clean up XML file in staging.gdb
# If not, next time, it will error because it can't output an XML file when one is present (no overwriting)
for f in in_features:
    try:
        os.remove(xml_file)
    except:
        pass
    
# Compress file gdb for sending -- nope, this doesn't create a zipped file
#arcpy.CompressFileGeodatabaseData_management(out_location, "Lossless compression")

#https://community.esri.com/t5/python-questions/zip-a-file-geodatabase-using-arcpy-or-zipfile/td-p/388286
#Creates the empty zip file and opens it for writing  

# This isn't working. Can't open the file gdb as directory in ArcGIS, 
# though outside of ArcGIS, it seems like it's finding the folder
def zip_gdb(input_gdb):
    gdb_file = str(input_gdb)
    out_file = gdb_file + '.zip'
    gdb_name = os.path.basename(gdb_file)
    
    with zipfile.ZipFile(out_file, mode='w', 
                         compression=zipfile.ZIP_DEFLATED, 
                         allowZip64=True) as myzip:
        for f in os.listdir(gdb_file):
            if f[-5:] != '.lock':
                myzip.write(os.path.join(gdb_file, f), gdb_name + '/' + os.path.basename(f))
    
    print('Completed zipping: {}'.format(gdb_file))

zip_gdb(out_location)


out_file = out_location + '.zip'
shutil.make_archive(out_file, 'zip', out_location)‍‍‍‍‍‍‍
