import geopandas as gpd
import pandas as pd
import _harmonization_utils 
from calitp_data_analysis.sql import to_snakecase
from calitp_data_analysis import utils

def sb1_basic_cleaning(
    gdf: gpd.GeoDataFrame, project_name_col: str, agency_col: str
) -> gpd.GeoDataFrame:
    """
    Perform basic cleaning before joining
    SB1 & Non SHOPP data together.
    """
    # Some project names contain the year. Remove anything after 20..
    gdf[project_name_col] = gdf[project_name_col].str.split("20").str[0]

    # Get rid of | in object cols
    # https://stackoverflow.com/questions/68152902/extracting-only-object-type-columns-in-a-separate-list-from-a-data-frame-in-pand
    string_cols = gdf.select_dtypes(include=["object"]).columns.to_list()
    try:
        for i in string_cols:
            gdf[i] = (
                gdf[i]
                .str.replace("|", "")
                .str.title()
                .str.replace("[^\w\s]", "")
                .str.strip()
            )
            gdf[i] = gdf[i].fillna("None")
    except:
        pass

    # Project agency always says "Los Angeles submitted by county"
    # or "Fremont submitted by city." Remove submitted by.
    gdf[agency_col] = gdf[agency_col].str.replace("Submitted By", " ")

    return gdf

def load_sb1() -> gpd.GeoDataFrame:
    """
    Load all the projects on the SB1 map from the Feature Server.

    https://odpsvcs.dot.ca.gov/arcgis/rest/services/SB1/SB1_ProjectData/FeatureServer
    """
    sb1_pt1 = "https://odpsvcs.dot.ca.gov/arcgis/rest/services/SB1/SB1_ProjectData/FeatureServer/"
    sb1_pt2 = "/query?where=1%3D1&objectIds=&time=&geometry=&geometryType=esriGeometryEnvelope&inSR=&spatialRel=esriSpatialRelIntersects&distance=&units=esriSRUnit_Foot&relationParam=&outFields=*+&returnGeometry=true&maxAllowableOffset=&geometryPrecision=&outSR=&gdbVersion=&historicMoment=&returnDistinctValues=false&returnIdsOnly=false&returnCountOnly=false&returnExtentOnly=false&orderByFields=&groupByFieldsForStatistics=&outStatistics=&returnZ=false&returnM=false&multipatchOption=&resultOffset=&resultRecordCount=&returnTrueCurves=false&sqlFormat=none&f=geojson"

    full_gdf = pd.DataFrame()
    for i in list(map(str, [*range(0, 16)])):
        df = to_snakecase(gpd.read_file(f"{sb1_pt1}{i}{sb1_pt2}"))
        full_gdf = pd.concat([full_gdf, df], axis=0)

    # Basic cleaning
    full_gdf = sb1_basic_cleaning(
        full_gdf,
        "projname",
        "projagency",
    )

    # Find missing geo
    missing_geo = full_gdf[(full_gdf.geometry.is_empty)]
    print(f"{len(missing_geo)} rows are mising geometry")

    # Find invalid geo
    invalid_geo = full_gdf[~full_gdf.geometry.is_valid].reset_index(drop=True)
    print(f"{len(invalid_geo)} rows contain invalid geography")

    # Filter out completed
    full_gdf = full_gdf.loc[full_gdf.projstatus != "Completed"].reset_index(drop=True)
    return full_gdf