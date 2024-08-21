import numpy as np
import pandas as pd
import shared_utils
from calitp_data_analysis.sql import to_snakecase
from _bus_cost_utils import GCS_PATH, new_prop_finder, new_bus_size_finder, project_type_finder, col_row_updater
import geopandas as gpd




def col_splitter(
    df: pd.DataFrame, 
    col_to_split: str, 
    new_col1: str, 
    new_col2: str, 
    split_char: str
)-> pd.DataFrame:
    """
    function to split a column into 2 columns by specific character.
    ex. split 100(beb) to "100" & "(beb)"
    """
    df[[new_col1, new_col2]] = df[col_to_split].str.split(
        pat=split_char, n=1, expand=True
    )

    df[new_col2] = df[new_col2].str.replace(")", "")

    return df

def fta_agg_bus_only(df: pd.DataFrame) -> pd.DataFrame:
    """
    filters FTA data to only show projects with bus procurement (bus count > 0).
    then filters projects for new_project_type = bus only
    then aggregates
    """
    df1 = df[(df["bus_count"] > 0) & (df["new_project_type"] == "bus only")]

    df2 = (
        df1.groupby(
            [
                "transit_agency",
                "project_title",
                "prop_type",
                "bus_size_type",
                "project_description",
                "new_project_type"
            ]
        )
        .agg(
            {
                "total_cost": "sum",
                "bus_count": "sum",
            }
        )
        .reset_index()
    )

    return df2

def clean_fta_columns() -> tuple:
    """
    Updated to read in FTA REST server data. Now reads in fiscal year 23 and 24 data.
    Read in data, renames columns, update specific values.
    """
    fy24 = "https://services.arcgis.com/xOi1kZaI0eWDREZv/ArcGIS/rest/services/FY2024_Bus_Awards_/FeatureServer/0/query?where=1%3D1&objectIds=&time=&geometry=&geometryType=esriGeometryPolygon&inSR=&spatialRel=esriSpatialRelIntersects&resultType=none&distance=0.0&units=esriSRUnit_Meter&relationParam=&returnGeodetic=false&outFields=*&returnGeometry=true&featureEncoding=esriDefault&multipatchOption=xyFootprint&maxAllowableOffset=&geometryPrecision=&outSR=&defaultSR=&datumTransformation=&applyVCSProjection=false&returnIdsOnly=false&returnUniqueIdsOnly=false&returnCountOnly=false&returnExtentOnly=false&returnQueryGeometry=false&returnDistinctValues=false&cacheHint=false&orderByFields=&groupByFieldsForStatistics=&outStatistics=&having=&resultOffset=&resultRecordCount=&returnZ=false&returnM=false&returnTrueCurves=false&returnExceededLimitFeatures=true&quantizationParameters=&sqlFormat=none&f=pgeojson&token="

    fy23 = "https://services.arcgis.com/xOi1kZaI0eWDREZv/ArcGIS/rest/services/2023_06_12_Awards/FeatureServer/0/query?where=1%3D1&objectIds=&time=&geometry=&geometryType=esriGeometryEnvelope&inSR=&spatialRel=esriSpatialRelIntersects&resultType=none&distance=0.0&units=esriSRUnit_Meter&relationParam=&returnGeodetic=false&outFields=*&returnGeometry=true&featureEncoding=esriDefault&multipatchOption=xyFootprint&maxAllowableOffset=&geometryPrecision=&outSR=&defaultSR=&datumTransformation=&applyVCSProjection=false&returnIdsOnly=false&returnUniqueIdsOnly=false&returnCountOnly=false&returnExtentOnly=false&returnQueryGeometry=false&returnDistinctValues=false&cacheHint=false&orderByFields=&groupByFieldsForStatistics=&outStatistics=&having=&resultOffset=&resultRecordCount=&returnZ=false&returnM=false&returnTrueCurves=false&returnExceededLimitFeatures=true&quantizationParameters=&sqlFormat=none&f=pgeojson&token="

    fy_24_data = to_snakecase(gpd.read_file(fy24))
    fy_23_data = to_snakecase(gpd.read_file(fy23))

    # cleaning fy23 data
    fy_23_data = fy_23_data.assign(
        extracted_prop_type=fy_23_data["project_description"].apply(
            _bus_cost_utils.new_prop_finder
        ),
        extracted_bus_size=fy_23_data["project_description"].apply(
            _bus_cost_utils.new_bus_size_finder
        ),
        fy="fy23",
    )

    fy_23_data["total_bus_count"] = fy_23_data[
        ["traditional_buses", "low_emission_buses", "zero_emission_buses"]
    ].sum(axis=1)
    fy_23_data["funding"] = (
        fy_23_data["funding"].str.replace("$", "").str.replace(",", "").astype("int64")
    )
    fy_23_data["zero_emission_buses"] = fy_23_data["zero_emission_buses"].astype(
        "int64"
    )

    col_list_23 = [
        "project_sponsor",
        "project_title",
        "project_description",
        "project_type",
        "funding",
        "total_bus_count",
        "extracted_prop_type",
        "extracted_bus_size",
        #"fy23"
    ]

    fy_23_bus = fy_23_data[(fy_23_data["project_type"] == "Bus")][col_list_23]

    fy_23_all_projects = fy_23_data[col_list_23]
    
    # cleaning the "not specified" rows
    _bus_cost_utils.col_row_updater(
        fy_23_bus,
        "project_title",
        "VA Rural Transit Asset Management and Equity Program",
        "extracted_prop_type",
        "mix (low emission)",
    )
    
    #cleaning fy24 data
    fy_24_data = fy_24_data.assign(
        extracted_prop_type=fy_24_data["project_description"].apply(_bus_cost_utils.new_prop_finder),
        extracted_bus_size=fy_24_data["project_description"].apply(_bus_cost_utils.new_bus_size_finder),
        fy="fy24",
    )
    
    col_list_24 = [
        "agency_name",
        "project_title",
        "project_description",
        "project_type",
        "funding_amount",
        "number_of_buses_",
        "extracted_prop_type",
        "extracted_bus_size",
        #"fy24"
    ]

    project_val = ["Vehicle ", "Vehicle"]

    prop_vals = [
        "Battery electric",
        "Hydrogen fuel cell",
        "Battery electric | Hydrogen fuel cell",
        "Battery electric ",
    ]

    fy_24_bus = fy_24_data[col_list_24][(fy_24_data["project_type"].isin(project_val))]

    fy_24_all_projects = fy_24_data[col_list_24]
    
    _bus_cost_utils.col_row_updater(fy_24_bus, "funding_amount", "2894131.0", "extracted_prop_type", "BEB")
    _bus_cost_utils.col_row_updater(fy_24_bus, "funding_amount", "14415095.0", "extracted_prop_type", "BEB")
    _bus_cost_utils.col_row_updater(fy_24_bus, "funding_amount", "18112632.0", "extracted_prop_type", "BEB")
    
    #cleaning before merging
    col_dict = {
        "project_sponsor": "transit_agency",
        "agency_name": "transit_agency",
        "project_type": "new_project_type",
        "funding": "total_cost",
        "funding_amount": "total_cost",
        "total_bus_count": "bus_count",
        "number_of_buses_": "bus_count",
        "extracted_prop_type": "prop_type",
        "extracted_bus_size": "bus_size_type",
    }
    
    fy_23_bus = fy_23_bus.rename(columns=col_dict)
    
    fy_24_bus = fy_24_bus.rename(columns=col_dict)
    
    # merge
    fta_all_projects_merge = pd.merge(fy_23_all_projects, fy_24_all_projects, how="outer")
    fta_bus_merge = pd.merge(fy_23_bus, fy_24_bus, how="outer")
    
    return fta_all_projects_merge, fta_bus_merge

if __name__ == "__main__":

    # initial dfs
    all_projects, just_bus = clean_fta_columns()

    # projects with bus count > 0 only.
    #just_bus = fta_agg_bus_only(just_bus)

    # export both DFs
    all_projects.to_parquet(f"{GCS_PATH}clean_fta_all_projects.parquet")
    just_bus.to_parquet(f"{GCS_PATH}clean_fta_bus_only.parquet")