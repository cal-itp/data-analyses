import pandas as pd
import _cleaning_utils 
from calitp_data_analysis.sql import to_snakecase

GCS_FILE_PATH = "gs://calitp-analytics-data/data-analyses/project_list/LRTP"

def ambag_lrtp():
    """
    Association of Monterey Bay Area Governments (AMBAG)
    """
    ambag1 = pd.read_excel(
        f"{LRTP_GCS}AMBAG_2045 Revenue Constrained Project List_Monterey County_by Project Type__022823.xlsx"
    )
    ambag2 = pd.read_excel(
        f"{LRTP_GCS}AMBAG_2045 Revenue Constrained Project List_San Benito County_by Project Type__022823.xlsx"
    )
    ambag3 = pd.read_excel(
        f"{LRTP_GCS}AMBAG_2045 Revenue Constrained Project List_Santa Cruz County_by Project Type__041923.xlsx"
    )

    # Move column names up
    ambag1 = embedded_column_names(ambag1, 1)
    ambag2 = embedded_column_names(ambag2, 1)
    ambag3 = embedded_column_names(ambag3, 1)

    # Add county
    ambag1["county"] = "Santa Cruz"
    ambag2["county"] = "Monterey"
    ambag3["county"] = "San Benito"

    # Conat & clean
    concat1 = pd.concat([ambag1, ambag2, ambag3], axis=0)
    concat1 = concat1.drop(columns=[2035.0, 2045.0, 2020.0, 2035.0])

    concat1 = to_snakecase(concat1)
    return concat1

def bcag_lrtp():
    """
    Project cost is in thousands
    Butte County Association of Governments (BCAG)
    """
    df = pd.read_excel(f"{LRTP_GCS}BCAG.xls")
    drop_columns = [
        2018,
        2020,
        2030,
        2035,
        2040,
        "EXEMPT CODE",
        "PM 1",
        "PM 2",
        "PM 3",
    ]

    df = df.drop(columns=drop_columns)
    df = to_snakecase(df)
    
    # Correct cost
    # df.cost_estimate = df.cost_estimate * 1_000
    df.fund_estimate = (
     df.fund_estimate
        .str.replace("$","")
        .str.replace(".","")
        .str.replace("million","")
        .apply(pd.to_numeric, errors="coerce") * 1_000_000)
    
    # create geometry
    df["geometry"] = gpd.GeoSeries.from_xy(
        df.x_coord, df.y_coord, crs=geography_utils.WGS84
    )
    # Same project is split across multiple rows. Divide out project cost
    df = _cleaning_utils.correct_project_cost(df, "title", "cost_estimate")
    df = df.set_geometry("geometry")

    return df

def fresnocog_lrtp():
    columns_to_drop = ["unnamed:_7", "unnamed:_8", "unnamed:_9"]
    df1 = to_snakecase(pd.read_excel(f"{LRTP_GCS}FRESNO_COG_UNconstrained.xlsx"))
    df1["financial_constraint"] = "unconstrained"

    df2 = to_snakecase(pd.read_excel(f"{LRTP_GCS}FRESNO_COG_Constrained.xlsx"))
    df2["financial_constraint"] = "constrained"

    concat1 = pd.concat([df1, df2], axis=0)
    concat1 = concat1.drop(columns=columns_to_drop)

    return concat1

def kcag_lrtp():
    """
    Kings County Association of Governments (KCAG)
    """
    df = to_snakecase(pd.read_excel(f"{LRTP_GCS}KCAG.xlsx"))

    # No title column
    df["title"] = (df.category + "-" + df.description + "-" + df.location).fillna(
        "No Title"
    )

    # Some duplicates
    df = df.drop_duplicates(["location", "category", "description"]).reset_index(
        drop=True
    )
    return df

def madera_lrtp():
    sheets_list = [
    "Streets and Roads",
    "Maintenance",
    "Safety",
    "ATP",
    "Public Transit",
    "Aviation",
    "ITS",
    "Unconstrained"]
    
    dict_df = pd.read_excel(f"{LRTP_GCS}Madera_CTC_cleaned.xlsx", sheet_name=sheets_list)
    df1 = dict_df.get(sheets_list[0])
    df2 = dict_df.get(sheets_list[1])
    df3 = dict_df.get(sheets_list[2])
    df4 = dict_df.get(sheets_list[3])
    df5 = dict_df.get(sheets_list[4])
    df6 = dict_df.get(sheets_list[5])
    df7 = dict_df.get(sheets_list[6])
    df8 = dict_df.get(sheets_list[7])
    
    concat1 = to_snakecase(pd.concat([df1,df2,df3,df4,df5,df6,df7,df8], axis= 0))
    
    concat1 = concat1.drop(columns = ['proje\nct_id'])
    return concat1

def mcagov_lrtp():
    """
    Merced County Association of Governments (MCAG)
    """
    df = to_snakecase(pd.read_excel(f"{LRTP_GCS}MCAGOV.xlsx"))

    df = df.dropna(subset=["title"]).reset_index(drop=True)

    return df

def mtc_lrtp():
    # Open rest server data
    layer_list = ["planbayarea2050_rtp_line", "planbayarea2050_rtp_point"]
    url_pt_1 = "https://services3.arcgis.com/i2dkYWmb4wHvYPda/ArcGIS/rest/services/"
    url_pt_2 = "/FeatureServer/0/query?where=1%3D1&objectIds=&time=&geometry=&geometryType=esriGeometryEnvelope&inSR=&spatialRel=esriSpatialRelIntersects&resultType=none&distance=0.0&units=esriSRUnit_Meter&relationParam=&returnGeodetic=false&outFields=*+&returnGeometry=true&featureEncoding=esriDefault&multipatchOption=xyFootprint&maxAllowableOffset=&geometryPrecision=&outSR=&defaultSR=&datumTransformation=&applyVCSProjection=false&returnIdsOnly=false&returnUniqueIdsOnly=false&returnCountOnly=false&returnExtentOnly=false&returnQueryGeometry=false&returnDistinctValues=false&cacheHint=false&orderByFields=&groupByFieldsForStatistics=&outStatistics=&having=&resultOffset=&resultRecordCount=&returnZ=false&returnM=false&returnExceededLimitFeatures=true&quantizationParameters=&sqlFormat=none&f=pgeojson&token="
    gdf = _cleaning_utils.open_rest_server(url_pt_1, url_pt_2, layer_list)

    # Open all MTC projects. Not all projects are mappable.
    all_projects_url = "https://data.bayareametro.gov/resource/y7ka-jksz.json?$query=SELECT%0A%20%20%60plan_strategy%60%2C%0A%20%20%60rtpid%60%2C%0A%20%20%60title%60%2C%0A%20%20%60scope%60%2C%0A%20%20%60open_period%60%2C%0A%20%20%60funding_millions_yoe%60%2C%0A%20%20%60county%60"
    all_projects = pd.read_json(all_projects_url)

    # Merge info from all projects to gdf
    gdf = pd.merge(
        gdf, all_projects, how="inner", left_on=["proj_title"], right_on=["title"]
    )

    gdf = gdf.drop(columns=["title"])

    # Find projects that are unmappable
    mtc_geometry_titles = set(gdf.proj_title.unique().tolist())
    mtc_all_projects_titles = set(all_projects.title.unique().tolist())
    missing_projects = list(mtc_all_projects_titles - mtc_geometry_titles)

    # Add back in the  unmappable projects
    all_projects = all_projects[all_projects.title.isin(missing_projects)].reset_index(
        drop=True
    )
    all_projects = all_projects.rename(columns={"title": "proj_title"})
    final = pd.concat([all_projects, gdf])

    # Correct typo
    final = final.rename({"mode detai": "mode_detail"})

    # Set geometry again
    final = final.set_geometry("geometry").set_crs(geography_utils.WGS84)

    # Same project is split across multiple rows. Divide out project cost
    final = correct_project_cost(final, "proj_title", "funding_millions_yoe")
    return final

def sacog_lrtp():
    """
    Sacramento Area Council of Governments (SACOG)
    """
    df = to_snakecase(pd.read_excel(f"{LRTP_GCS}SACOG.xlsx"))

    # Delete embedded headers
    df = delete_embedded_headers(df, "description", "Description")

    # Delete columns with all nulls
    df = df.dropna(axis=1, how="all")

    # Drop duplicates
    df = df.drop_duplicates().reset_index(drop=True)
    return df

def scag_lrtp():
    """
    Southern California Association of Governments (SCAG)
    """
    sheets_list = ["Table 1", "Table 2"]
    dict_df = pd.read_excel(f"{LRTP_GCS}SCAG.xlsx", sheet_name=sheets_list)

    df1 = to_snakecase(dict_df.get("Table 1"))
    # Drop columns where everything is NA
    df1 = df1.dropna(axis=1, how="all")

    df2 = to_snakecase(dict_df.get("Table 2"))
    df2 = df2.dropna(axis=1, how="all")

    # Concat
    df3 = pd.concat([df1, df2])

    df4 = _cleaning_utils.delete_embedded_headers(df3, "rtp_id", "RTP ID")

    df4 = df4.dropna(
        subset=["description", "lead_agency", "project_cost__$1,000s_"]
    ).reset_index(drop=True)

    # No project title
    df4["project_title"] = (df4.description + "-" + df4.lead_agency).fillna("No Title")

    # Project cost is in thousands.
    # Multiple out
    df4["project_cost_millions"] = df4["project_cost__$1,000s_"] * 1_000
    return df4

def sandag_lrtp():
    """
    San Diego Association of Governments (SANDAG)
    """
    sandag_layers_list = [
        "Complete_Corridors_Connectors_Intersections_Ramps_RP2021",
        "Complete_Corridors_Highway_RP2021",
        "Transit_Leap_RP2021",
        "Adopted_Regional_Bike_Network_RP2021",
        "Mobility_Hubs_and_Flexible_Fleets",
        "Complete_Corridors_Regional_Arterials",
        "Goods_Movement",
    ]

    sandag_url_pt1 = (
        "https://services1.arcgis.com/HG80xaIVT1z1OdO5/ArcGIS/rest/services/"
    )
    sandag_url_pt2 = "/FeatureServer/0/query?where=1%3D1&objectIds=&time=&geometry=&geometryType=esriGeometryEnvelope&inSR=&spatialRel=esriSpatialRelIntersects&resultType=none&distance=0.0&units=esriSRUnit_Meter&relationParam=&returnGeodetic=false&outFields=*+&returnGeometry=true&featureEncoding=esriDefault&multipatchOption=xyFootprint&maxAllowableOffset=&geometryPrecision=&outSR=&defaultSR=&datumTransformation=&applyVCSProjection=false&returnIdsOnly=false&returnUniqueIdsOnly=false&returnCountOnly=false&returnExtentOnly=false&returnQueryGeometry=false&returnDistinctValues=false&cacheHint=false&orderByFields=&groupByFieldsForStatistics=&outStatistics=&having=&resultOffset=&resultRecordCount=&returnZ=false&returnM=false&returnExceededLimitFeatures=true&quantizationParameters=&sqlFormat=none&f=pgeojson&token="
    sandag = open_rest_server(sandag_url_pt1, sandag_url_pt2, sandag_layers_list)

    # Same project is split across multiple rows. Divide out project cost
    sandag.cost2020m = (
        sandag.cost2020m.str.replace("$", "")
        .str.replace("N/A", "")
        .apply(pd.to_numeric, errors="coerce")
    )
    sandag = _cleaning_utils.correct_project_cost(sandag, "project_name", "cost2020m")

    return sandag

def sbcag_lrtp():
    """
    Santa Barbara County Association of Governments (SBCAG)
    """
    df = to_snakecase(pd.read_excel(f"{LRTP_GCS}SBCAG.xlsx"))
    drop_columns = [
        "unnamed:_2",
        "project_type",
        "unnamed:_4",
        "unnamed:_6",
        "unnamed:_8",
        "unnamed:_9",
        "unnamed:_10",
        "unnamed:_13",
    ]
    df = df.drop(columns=drop_columns)
    df = _cleaning_utils.delete_embedded_headers(df, "description", "Description")
    df = df.dropna(subset=["description"]).reset_index(drop=True)

    df = df.drop_duplicates(
        subset=["phase", "project_title", "phase", "total_cost__$000s_"]
    ).reset_index(drop=True)
    
    df["project_cost_millions"] = df["total_cost__$000s_"] * 1_000
    return df

def sjcog_lrtp():
    """
    San Joaquin Council of Governments (SJCOG)
    """
    sheets_list = [
        "1 Mainline",
        "2 Interchanges",
        "3 Reg Roadways",
        "4 RR Xings",
        "5 Bus Transit",
        "6 Rail Corridor",
        "7 Airports",
        "8 Active Trans Facilities",
        "9 TCMs",
        "10 Ops & Maint",
    ]
    dict_df = pd.read_excel(f"{LRTP_GCS}SJCOG.xlsx", sheet_name=sheets_list)
    df1 = to_snakecase(dict_df.get("1 Mainline"))
    df2 = to_snakecase(dict_df.get("2 Interchanges"))
    df3 = to_snakecase(dict_df.get("3 Reg Roadways"))
    df4 = to_snakecase(dict_df.get("4 RR Xings"))
    df5 = to_snakecase(dict_df.get("5 Bus Transit"))
    df6 = to_snakecase(dict_df.get("6 Rail Corridor"))
    df7 = to_snakecase(dict_df.get("7 Airports"))
    df8 = to_snakecase(dict_df.get("8 Active Trans Facilities"))
    df9 = to_snakecase(dict_df.get("9 TCMs"))
    df10 = to_snakecase(dict_df.get("10 Ops & Maint"))

    concat1 = pd.concat([df1, df2, df3, df4, df5, df6, df7, df8, df9, df10], axis=0)

    sjcog_cols = [
        "_2018_rtp_mpo_id",
        "ctips_id_#",
        "jurisdiction",
        "facility_name_route",
        "project_description",
        "project_limits",
        "total",
        "ftip_programming",
        "nepa_approval",
        "open_to_traffic",
        "completion",
        "completion_date",
        "facility_name_route_1",
        "project_description_1",
        "milestone_years_1",
        "project_name",
    ]

    concat1 = concat1.dropna(
        subset=[
            "project_description",
            "_2018_rtp_mpo_id",
        ]
    ).reset_index(drop=True)

    # Some projects have no titles, create them manually
    concat1["title_manual"] = (
        concat1.project_limits
        + "-"
        + concat1.jurisdiction
        + "-"
        + concat1.facility_name_route
    )
    concat1.title_manual = concat1.title_manual.fillna("No Title")
    concat1.project_name = concat1.project_name.fillna(concat1.title_manual)

    concat1 = concat1[sjcog_cols]

    return concat1

def stancog_lrtp():
    """
    Stanislaus Council of Governments (StanCOG)
    """
    df = to_snakecase(pd.read_excel(f"{LRTP_GCS}STANCOG.xlsx"))
    df = embedded_column_names(df, 1)
    keep_columns = [
        "Jurisdiction",
        "Location",
        "Project Limits",
        "Description",
        "Total Cost",
        "Open to Traffic",
        "Funding Source",
        "System Preserv.",
        "Capacity Enhance.",
        "Safety",
        "Oper.",
        "Complete Streets",
        "Active\nTransporta tion",
        "Transit",
        "Other",
    ]

    df = df[keep_columns]

    df = to_snakecase(df)

    df = df.dropna(subset=["description"]).reset_index(drop=True)

    df["title"] = (df.location + "-" + df.description).fillna("No Title")

    df = _cleaning_utils.delete_embedded_headers(df, "location", "Location")

    df = df.drop_duplicates(
        subset=["title", "location", "jurisdiction", "total_cost", "open_to_traffic"]
    ).reset_index(drop=True)
    return df

def tmpo_lrtp():
    """
    Tahoe Metropolitan Planning Organization (TMPO)
    """
    tahoe_url = "https://maps.trpa.org/server/rest/services/Datadownloader_Transportation/MapServer/19/query?where=1%3D1&text=&objectIds=&time=&timeRelation=esriTimeRelationOverlaps&geometry=&geometryType=esriGeometryEnvelope&inSR=&spatialRel=esriSpatialRelIntersects&distance=&units=esriSRUnit_Foot&relationParam=&outFields=*+&returnGeometry=true&returnTrueCurves=false&maxAllowableOffset=&geometryPrecision=&outSR=&havingClause=&returnIdsOnly=false&returnCountOnly=false&orderByFields=&groupByFieldsForStatistics=&outStatistics=&returnZ=false&returnM=false&gdbVersion=&historicMoment=&returnDistinctValues=false&resultOffset=&resultRecordCount=&returnExtentOnly=false&sqlFormat=none&datumTransformation=&parameterValues=&rangeValues=&quantizationParameters=&featureEncoding=esriDefault&f=geojson"
    gdf = to_snakecase(gpd.read_file(tahoe_url))
    gdf.complete_year = gdf.complete_year.astype(int)

    # Filter out projects that are not yet completed
    gdf2 = gdf[gdf.complete_year > 2022].reset_index(drop=True)

    gdf2["project_year"] = (
        gdf2.start_year.astype(str) + "-" + gdf.complete_year.astype(str)
    )
    return gdf2

def tcag_lrtp():
    """
    Tulare County Association of Governments (TCAG)
    """
    df = to_snakecase(pd.read_excel(f"{LRTP_GCS}TCAG.xlsx"))

    def completion_2035(row):
        if row.completed_by_2035_y_n == "Y":
            return "< 2035"

    df["completion_year"] = df.apply(lambda x: completion_2035(x), axis=1)
    df['cost_in_millions'] = df['cost_x_1,000']*1_000
    return df