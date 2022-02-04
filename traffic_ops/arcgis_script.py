# Save a version of script that runs within ArcGIS
import arcpy
import os

print os.getcwd()
# Set this path to be in _env
arcpy.env.workspace = r"C:\Users\s153936\Documents\ArcGIS"

# Set local variables
in_features = ['stops_assembled/stops_assembled.shp', 
               'routes_assembled/routes_assembled.shp']
out_location = 'open_data.gdb'

# Execute FeatureClassToGeodatabase
arcpy.FeatureClassToGeodatabase_conversion(in_features, out_location)

# Export metadata using FGDC
translator =  'C:\Program Files (x86)\ArcGIS\Desktop10.8\Metadata\Translator\ArcGIS2FGDC.xml'

arcpy.ExportMetadata_conversion (out_location + '/routes_assembled', translator, 
    'routes_assembled.xml')
arcpy.ExportMetadata_conversion (out_location + '/stops_assembled', translator, 
    'stops_assembled.xml')
