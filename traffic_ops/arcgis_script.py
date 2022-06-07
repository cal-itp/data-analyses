# Save a version of script that runs within ArcGIS
import arcpy
import os

print os.getcwd()
# Set this path to be in _env
arcpy.env.workspace = r"C:\Users\s153936\Documents\ArcGIS"

# Set local variables
in_features = [
    #'stops_assembled', 'routes_assembled',
    'ca_hq_transit_areas',
    #'ca_hq_transit_stops',
]

out_location = 'open_data.gdb'

# Path to Metadata stylesheet
directory = arcpy.GetInstallInfo("desktop")["InstallDir"] 
#"c:\program files (x86)\arcgis\desktop10.8\"

# Export metadata using FGDC
translator =  directory + 'Metadata\Translator\ArcGIS2FGDC.xml'


for f in in_features:
    # construct the filename, which is takes form of routes_assembled/routes_assembled.shp
    file_name = f + '/' + f + '.shp'

    # Execute FeatureClassToGeodatabase
    #arcpy.FeatureClassToGeodatabase_conversion(file_name, out_location)
    
    # Construct XML filename
    #xml_file = f + '_orig.xml'
    updated_xml_file = f + '.xml'
    
    #arcpy.ExportMetadata_conversion (out_location + '/' + f, translator, 
    #    xml_file)
    
    # Upgrade from FGDC to ArcGIS (this works)
    #arcpy.conversion.UpgradeMetadata(updated_xml_file, "FGDC_TO_ARCGIS")

    arcpy.conversion.ImportMetadata(updated_xml_file, "FROM_ARCGIS", f, "ENABLED")

    #arcpy.SynchronizeMetadata_conversion("vegetation", "NOT_CREATED")

    arcpy.MetadataImporter_conversion(updated_xml_file, f)
    ## BELOW - NOT TESTED YET
    #Source_Metadata = arcpy.GetParameter(0)
    
    #for item in Source_Metadata:
    #    print(item)

    
    # https://desktop.arcgis.com/en/arcmap/latest/manage-data/metadata/editing-metadata-for-many-arcgis-items.htm
    # Add changes in metadata and write it to the file
    #arcpy.MetadataImporter_conversion(xml_file, item)
