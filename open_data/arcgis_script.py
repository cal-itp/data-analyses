"""
Script to run in ArcGIS.
More limited because it relies on `arcpy`
"""
## Run this in Hub -- dotenv cannot be run in ArcGIS
# Set this path to be in _env
# Hardcode it in the script within ArcGIS
import dot_env
dotenv.load_dotenv("_env")
ARCGIS_PATH = os.environ["ARCGIS_PATH"]


# Save a version of script that runs within ArcGIS
import arcpy
import os
import zipfile

arcpy.env.workspace = ARCGIS_PATH

# Set local variables
in_features = [
    'ca_hq_transit_areas',
    'ca_hq_transit_stops',
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
#arcpy.CompressFileGeodatabaseData_management(out_location, "Lossless compression")

#https://community.esri.com/t5/python-questions/zip-a-file-geodatabase-using-arcpy-or-zipfile/td-p/388286
#Creates the empty zip file and opens it for writing       
def zip_gdb(input_gdb):
    gdb_file = str(input_gdb)
    out_file = gdb_file[0:-4] + '.zip'
    gdb_name = os.path.basename(gdb_file)
    
    with zipfile.ZipFile(out_file, mode='w', 
                         compression=zipfile.ZIP_DEFLATED, 
                         allowZip64=True) as myzip:
        for f in os.listdir(gdb_file):
            if f[-5:] != '.lock':
                myzip.write(os.path.join(gdb_file, f), gdb_name + '/' + os.path.basename(f))
    
    print('Completed zipping: {}'.format(gdb_file))

zip_gdb(out_location)