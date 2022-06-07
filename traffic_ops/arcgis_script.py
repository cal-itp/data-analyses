# Save a version of script that runs within ArcGIS
import arcpy
import os

print os.getcwd()
# Set this path to be in _env
arcpy.env.workspace = r"C:\Users\s153936\Documents\ArcGIS"

# Set local variables
in_features = [
    #'stops_assembled', 'routes_assembled',
    #'ca_hq_transit_areas',
    'ca_hq_transit_stops',
]

out_location = 'open_data.gdb'

# Path to Metadata stylesheet
directory = arcpy.GetInstallInfo("desktop")["InstallDir"] 
#"c:\program files (x86)\arcgis\desktop10.8\"

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
    # This is the one after it's manually changed. Keep separate to see what works.
    updated_xml_file = f + '/' + f + '.xml'
    
    arcpy.ExportMetadata_conversion (out_location + '/' + f, 
                                     translator, 
                                     out_location + '/' + xml_file)
    
    # Upgrade from FGDC to ArcGIS (this works...need this step?)
    # But following step imports it from FGDC...confusing
    #arcpy.conversion.UpgradeMetadata(updated_xml_file, "FGDC_TO_ARCGIS")
    
    # Import XML into file gdb
    #arcpy.conversion.MetadataImporter(updated_xml_file, f)

    # My updated xml is in the shapefile folder (separate from original)
    
    # Import the updated xml, then overwrite the metadata in the file gdb    
    arcpy.conversion.ImportMetadata(updated_xml_file, 
                                    "FROM_FGDC", 
                                    out_location + '/' + f, 
                                    "ENABLED")


    
    # https://desktop.arcgis.com/en/arcmap/latest/manage-data/metadata/editing-metadata-for-many-arcgis-items.htm
    # Add changes in metadata and write it to the file
