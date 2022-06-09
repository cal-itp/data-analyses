# Save a version of script that runs within ArcGIS
import arcpy
import os

print os.getcwd()
# Set this path to be in _env
arcpy.env.workspace = r"C:\Users\s153936\Documents\ArcGIS"

# Set local variables
in_features = [
    'ca_hq_transit_areas',
    #'ca_hq_transit_stops',
]

out_location = 'open_data.gdb'

# Path to Metadata stylesheet
directory = arcpy.GetInstallInfo("desktop")["InstallDir"] 

# Export metadata using FGDC
translator =  directory + 'Metadata\Translator\ArcGIS2FGDC.xml'


for f in in_features:
    # construct the filename, which is takes form of routes_assembled/routes_assembled.shp
    shp_file_name = f + '/' + f + '.shp'

    # Execute FeatureClassToGeodatabase
    arcpy.FeatureClassToGeodatabase_conversion(shp_file_name, out_location)
    
    # Construct XML filename
    # Spit out one that's before it's manually changed
    xml_file = f + '.xml'
    
    arcpy.ExportMetadata_conversion (out_location + '/' + f, 
                                     translator, 
                                     out_location + '/' + xml_file)
    


### UPDATE XML METADATA SEPARATELY IN PYTHON OUTSIDE OF ARCGIS


for f in in_features:
    # This is the one after it's manually changed. Keep separate to see what works.
    updated_xml_file = f + '/' + f + '.xml'
            
    # Import the updated xml, then overwrite the metadata in the file gdb    
    arcpy.conversion.ImportMetadata(updated_xml_file, 
                                    "FROM_FGDC", 
                                    out_location + '/' + f, 
                                    "ENABLED")
    

# Compress file gdb for sending -- nope, this doesn't create a zipped file
# Manually create zipped .gdb file
arcpy.CompressFileGeodatabaseData_management(out_location, "Lossless compression")
