# Save a version of script that runs within ArcGIS
import arcpy
import os

print os.getcwd()
# Set this path to be in _env
arcpy.env.workspace = r"C:\Users\s153936\Documents\ArcGIS"

# Set local variables
in_features = [
    #'stops_assembled', 'routes_assembled',
    'ca_hq_transit_areas','ca_hq_transit_stops',
]

out_location = 'open_data.gdb'

# Execute FeatureClassToGeodatabase
#arcpy.FeatureClassToGeodatabase_conversion(in_features, out_location)

# Export metadata using FGDC
translator =  'C:\Program Files (x86)\ArcGIS\Desktop10.8\Metadata\Translator\ArcGIS2FGDC.xml'

for f in in_features:
    # construct the filename, which is takes form of routes_assembled/routes_assembled.shp
    file_name = f + '/' + f + '.shp'

    # Execute FeatureClassToGeodatabase
    arcpy.FeatureClassToGeodatabase_conversion(file_name, out_location)
    
    # Construct XML filename
    xml_file = f + '.xml'

    arcpy.ExportMetadata_conversion (out_location + '/' + f, translator, 
        xml_file)
    

