import pandas as pd
import geopandas as gpd
import _cleaning_utils 
from calitp_data_analysis.sql import to_snakecase
import _harmonization_utils as harmonization_utils
import fsspec
LRTP_GCS = f"{harmonization_utils.GCS_FILE_PATH}LRTP/"

"""
General Functions
"""
def give_info(df, project_title: str, other_descriptive_col: str):
    """
    Return some basic info and flag duplicates
    for a dataframe
    """
    print(df[project_title].value_counts().head())
    print(f"# of unique project titles: {df[project_title].nunique()}")
    print(
        f"After dropping duplicates using {project_title} and {other_descriptive_col}: {len(df.drop_duplicates(subset = [project_title, other_descriptive_col]))}"
    )
    print(f"Df shape: {df.shape}")
    print(df.columns)
    
def group_duplicates(df:pd.DataFrame, group_col1:str, group_col2:str, agg_col1:str):
    
     display(df
     .groupby([group_col1, group_col2])
     .agg({agg_col1:'count'})
     .sort_values(by = [agg_col1], ascending = False)
     .head(10)
    )    
    
def harmonizing_lrtp(
    df,
    project_name_col: str,
    project_description_col: str,
    project_cost_col: str,
    location_col: str,
    county_col: str,
    city_col: str,
    project_year_col: str,
    data_source: str,
    lead_agency:str,
    note_cols: list,
    cost_in_millions: bool = True,
):
    """
    Take a dataset and change the column names/types to
    the same names and formats.
    """
    rename_columns = {
        project_name_col: "project_title",
        project_description_col: "project_description",
        project_cost_col: "total_project_cost",
        location_col: "geometry",
        county_col: "county",
        city_col: "city",
        project_year_col: "project_year",
        lead_agency: 'lead_agency',
    }
    # Rename columns
    df = df.rename(columns=rename_columns)

    # Coerce cost/fund columns to right type
    cost_columns = df.columns[df.columns.str.contains("(cost|funds)")].tolist()

    for i in cost_columns:
        df[i] = df[i].apply(pd.to_numeric, errors="coerce")

    # Add MPO & grant program
    df["lead_agency"] = data_source

    # Add data source
    df["data_source"] = f"{data_source} LRTP"

    # Divide cost columns by millions
    # If bool is set to True
    """
    if cost_in_millions:
        for i in cost_columns:
            df[i] = df[i].divide(1_000_000)
    """
    
    # Create columns even if they don't exist, just to harmonize
    # before concatting.
    create_columns = [
        "county",
        "city",
        "notes",
        "project_year",
        "project_description"
    ]

    for column in create_columns:
        if column not in df:
            df[column] = "None"
    if "geometry" not in df:
        df["geometry"] = None
    if "total_project_cost" not in df:
        df["total_project_cost"]: 0
    if "lead_agency" not in df:
        df["lead_agency"] = program
        
    # Create notes
    df = create_notes(df, note_cols)

    columns_to_keep = [
        "project_title",
        "lead_agency",
        "project_year",
        "project_description",
        "total_project_cost",
        "geometry",
        "city",
        "county",
        "data_source",
        "notes",
    ]

    df = df[columns_to_keep]

    return df

def embedded_column_names(df, data_start: int) -> pd.DataFrame:
    """
    Some excel sheets have headers and column names
    embedded in the dataframe. Take them out.

    Args:
        data_start: the row number the column names begin.
    """
    # Delete header
    df = df.iloc[data_start:].reset_index(drop=True)
    
    # The first row contains column names - update it to the column
    df.columns = df.iloc[0]

    # Drop the first row as they are now column names
    df = df.drop(df.index[0]).reset_index(drop=True)

    return df

def create_notes(df, note_cols: list):
    """
    Combine multiple columns together
    into a single column to minimize width.
    """
    prefix = "_"
    for column in note_cols:
        df[f"{prefix}{column}"] = df[column].astype(str)
    note_cols = [prefix + sub for sub in note_cols]

    # https://stackoverflow.com/questions/65532480/how-to-combine-column-names-and-values
    def combine_notes(x):
        return ", ".join([col + ": " + x[col] for col in note_cols])

    df["notes"] = df.apply(combine_notes, axis=1)
    df.notes = df.notes.str.replace("_", " ")

    return df

def open_rest_server(url_pt_1: str, url_pt_2: str, layer_name: list):
    """
    Open up data that is availably publicly via ArcGis
    """
    full_gdf = pd.DataFrame()
    for i in layer_name:
        gdf = to_snakecase(gpd.read_file(f"{url_pt_1}{i}{url_pt_2}"))
        gdf["layer_name"] = i
        full_gdf = pd.concat([full_gdf, gdf], axis=0)

    return full_gdf

def delete_embedded_headers(df, column: str, string_search: str) -> pd.DataFrame:
    """
    Some PDFS include the column names embedded mulitple times
    within the df. Delete them out.

    Example: Under the column 'description', delete the rows
    in which the value is 'description.' This signals that the row
    is just repeating the column name again.
    """
    headers = df[df[column].str.contains(string_search) == True]
    headers_index_list = headers.index.values.tolist()

    print(f"{len(headers_index_list)} rows are headers")

    df2 = df.drop(headers_index_list).reset_index(drop=True)
    return df2

def correct_project_cost(df, project_title_col: str, project_total_cost: str):
    """
    For some datasets, the same project
    (as determined by the same project name, cost,
    and source) is split across multiple rows.

    Ex: A project costs $500 million and is
    split on 5 rows by phase/location. Each row still lists
    the total  cost as $500 million, which is not accurate.
    This function will recalculate each of the row to list
    $100 mil as the total project cost
    """
    # Create a unique identifier
    df["unique_identifier"] = df[project_title_col] + df[project_total_cost].astype(str)

    # Create count for each project
    df["how_many_times_same_proj_appears"] = (
        df.groupby("unique_identifier").cumcount() + 1
    )

    # Find the total number of times a project title-cost appears.
    # Sort by descending and keep only the row with the highest level
    keep_cols = [
        project_title_col,
        "how_many_times_same_proj_appears",
        project_total_cost,
        "unique_identifier",
    ]
    df2 = (
        df[keep_cols]
        .sort_values(
            [project_title_col, "how_many_times_same_proj_appears"], ascending=False
        )
        .drop_duplicates(subset=["unique_identifier"])
    )

    # Create new funding estimate
    df2["new_proj_cost"] = (
        df2[project_total_cost] / df2["how_many_times_same_proj_appears"]
    )

    # Drop some columns
    df2 = df2.drop(
        columns=[
            project_title_col,
            project_total_cost,
            "how_many_times_same_proj_appears",
        ]
    )

    # Merge
    m1 = pd.merge(df, df2, how="inner", on="unique_identifier")

    # Clean up
    m1 = m1.drop(
        columns=[
            "unique_identifier",
            "how_many_times_same_proj_appears",
            project_total_cost,
        ]
    )

    # Replace project cost
    m1 = m1.rename(columns={"new_proj_cost": "total_project_cost"})
    return m1

"""
Individual LRTP
"""  
def ambag_lrtp():
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

    # Millions
    concat1["total_cost_millions"] = concat1["total_cost_\n_$_000s_"] * 1_000
    return concat1

def harmonize_ambag():
    df = ambag_lrtp()
    df = harmonizing_lrtp(
        df,
        project_name_col="project",
        project_description_col="project_description",
        project_cost_col="total_cost_millions",
        location_col="",
        county_col="county",
        city_col="",
        project_year_col="",
        data_source="AMBAG",
        lead_agency = "",
        note_cols= [],
        cost_in_millions=True,
    )

    return df

def bcag_lrtp():
    """
    Project cost is in thousands
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
    df.cost_estimate = df.cost_estimate * 1_000
    
    # Can't seem to correct fund_estimate bc
    # of so many variances
    """
    df.fund_estimate = (
        df.fund_estimate.str.replace("$", "")
        .str.replace(".", "")
        .str.replace("million", "")
        .apply(pd.to_numeric, errors="coerce")
        * 1_000
    )
    """
    
    # create geometry
    df["geometry"] = gpd.GeoSeries.from_xy(df.x_coord, df.y_coord, crs="EPSG:4326")
    
    # Same project is split across multiple rows. Divide out project cost
    df = correct_project_cost(df, "title", "cost_estimate")
    df = df.set_geometry("geometry")

    return df

def harmonize_bcag():
    df = bcag_lrtp()
    df = harmonizing_lrtp(
        df,
        project_name_col="title",
        project_description_col="project_descr",
        project_cost_col="total_project_cost",
        location_col="geometry",
        county_col="",
        city_col="",
        project_year_col="target_fiscal_year",
        data_source="BCAG",
        lead_agency = "agency",
        note_cols=[
            "project_type",
            "status",
            "fund_estimate",
            "fund_source",
        ],
        cost_in_millions=True,
    )

    return df

def fresnocog_lrtp():
    columns_to_drop = ["unnamed:_7", "unnamed:_8", "unnamed:_9"]
    df1 = to_snakecase(pd.read_excel(f"{LRTP_GCS}FRESNO_COG_UNconstrained.xlsx"))
    df1["financial_constraint"] = "unconstrained"

    df2 = to_snakecase(pd.read_excel(f"{LRTP_GCS}FRESNO_COG_Constrained.xlsx"))
    df2["financial_constraint"] = "constrained"

    concat1 = pd.concat([df1, df2], axis=0)
    concat1 = concat1.drop(columns=columns_to_drop)
    concat1.project_title = concat1.project_title.fillna("None")
    return concat1

def harmonize_fresnocog():
    df = fresnocog_lrtp()
    df = harmonizing_lrtp(
        df,
        project_name_col="project_title",
        project_description_col="project_description",
        project_cost_col="est_total_project_cost",
        location_col="",
        county_col="",
        city_col="",
        project_year_col="estimated_open_to_traffic",
        data_source="Fresno COG",
        lead_agency="",
        note_cols=[
            "project_type",
            "financial_constraint",
        ],
        cost_in_millions=True,
    )

    return df

def kcag_lrtp():

    df = to_snakecase(pd.read_excel(f"{LRTP_GCS}KCAG.xlsx"))

    # No title column
    # df["title"] = (df.category + "-" + df.location).fillna("No Title")
    df["title"] = "None"
    
    # Some duplicates
    df = df.drop_duplicates(["location", "category", "description"]).reset_index(
        drop=True
    )

    # Create cost
    df["total_cost"] = 0

    return df

def harmonize_kcag():
    df = kcag_lrtp()
    df = harmonizing_lrtp(
        df,
        project_name_col="title",
        project_description_col="description",
        project_cost_col="total_cost",
        location_col="",
        county_col="",
        city_col="",
        project_year_col="",
        data_source="KCAG",
        lead_agency="",
        note_cols=[
            "category",
            "location",
            "jurisdiction",
            "state_route",
            "post_mile",
            "project_limits",
        ],
        cost_in_millions=True,
    )

    return df

def kern_lrtp():
    to_keep = [
        "project_title",
        "scope",
        "yoe_w__new_revenue",
        "yoe_w_o_new_reven",
        "maint__inflation_savings",
    ]

    monetary_cols = [
        "yoe_w__new_revenue",
        "yoe_w_o_new_reven",
        "maint__inflation_savings",
    ]

    df = to_snakecase(pd.read_excel(f"{LRTP_GCS}KERNCOG.xlsx", sheet_name="Cleaned"))

    df = df[to_keep]

    df[monetary_cols] = df[monetary_cols] * 1_000

    df["cost"] = df.yoe_w__new_revenue

    df.cost = df.cost.fillna(df.yoe_w_o_new_reven)

    df = df.drop_duplicates(["project_title", "scope", "cost"]).reset_index(drop=True)

    return df

def harmonize_kerncog():
    df = kern_lrtp()
    df = harmonizing_lrtp(
        df,
        project_name_col="project_title",
        project_description_col="scope",
        project_cost_col="cost",
        location_col="",
        county_col="",
        city_col="",
        project_year_col="",
        data_source="Kern Cog",
        lead_agency="",
        note_cols=[
            "yoe_w__new_revenue",
            "yoe_w_o_new_reven",
        ],
        cost_in_millions=True,
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
        "Unconstrained",
    ]

    dict_df = pd.read_excel(
        f"{LRTP_GCS}Madera_CTC_cleaned.xlsx", sheet_name=sheets_list
    )
    df1 = dict_df.get(sheets_list[0])
    df2 = dict_df.get(sheets_list[1])
    df3 = dict_df.get(sheets_list[2])
    df4 = dict_df.get(sheets_list[3])
    df5 = dict_df.get(sheets_list[4])
    df6 = dict_df.get(sheets_list[5])
    df7 = dict_df.get(sheets_list[6])
    df8 = dict_df.get(sheets_list[7])

    concat1 = to_snakecase(pd.concat([df1, df2, df3, df4, df5, df6, df7, df8], axis=0))

    concat1 = concat1.drop(columns=["proje\nct_id"])
    return concat1

def harmonize_madera():
    df = madera_lrtp()
    df = harmonizing_lrtp(
        df,
        project_name_col="project_name",
        project_description_col="description",
        project_cost_col="total_cost",
        location_col="",
        county_col="",
        city_col="",
        project_year_col="opening_year",
        data_source="Madera CTC",
        lead_agency="agency",
        note_cols=[
            "category",
            "location",
        ],
        cost_in_millions=True,
    )

    return df

def mcagov_lrtp():

    df = to_snakecase(pd.read_excel(f"{LRTP_GCS}MCAGOV.xlsx"))

    df = df.dropna(subset=["title"]).reset_index(drop=True)

    # Millions
    df["total_cost_millions"] = df["total_cost\n_$1,000s_"] * 1_000
    return df

def harmonize_mcagov():
    df = mcagov_lrtp()
    df = harmonizing_lrtp(
        df,
        project_name_col="title",
        project_description_col="limits_description",
        project_cost_col="total_cost_millions",
        location_col="",
        county_col="",
        city_col="",
        project_year_col="completion\nyear",
        data_source="MCAGOV",
        lead_agency="agency",
        note_cols=[
            "type",
            "funding_sources",
        ],
        cost_in_millions=True,
    )

    return df

def mtc_lrtp():
    # Open rest server data
    layer_list = ["planbayarea2050_rtp_line", "planbayarea2050_rtp_point"]
    url_pt_1 = "https://services3.arcgis.com/i2dkYWmb4wHvYPda/ArcGIS/rest/services/"
    url_pt_2 = "/FeatureServer/0/query?where=1%3D1&objectIds=&time=&geometry=&geometryType=esriGeometryEnvelope&inSR=&spatialRel=esriSpatialRelIntersects&resultType=none&distance=0.0&units=esriSRUnit_Meter&relationParam=&returnGeodetic=false&outFields=*+&returnGeometry=true&featureEncoding=esriDefault&multipatchOption=xyFootprint&maxAllowableOffset=&geometryPrecision=&outSR=&defaultSR=&datumTransformation=&applyVCSProjection=false&returnIdsOnly=false&returnUniqueIdsOnly=false&returnCountOnly=false&returnExtentOnly=false&returnQueryGeometry=false&returnDistinctValues=false&cacheHint=false&orderByFields=&groupByFieldsForStatistics=&outStatistics=&having=&resultOffset=&resultRecordCount=&returnZ=false&returnM=false&returnExceededLimitFeatures=true&quantizationParameters=&sqlFormat=none&f=pgeojson&token="
    gdf = open_rest_server(url_pt_1, url_pt_2, layer_list)

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
    final = final.set_geometry("geometry").set_crs("EPSG:4326")
    
    # Convert to full number instead of truncated
    final['funding_numerated'] = final.funding_millions_yoe * 1_000_000

    # Same project is split across multiple rows. Divide out project cost
    final = correct_project_cost(final, "proj_title", "funding_numerated")

    # Divide project cost over 30 years
    final.total_project_cost = final.total_project_cost / 30
    return final

def harmonize_mtc():
    df = mtc_lrtp()
    df = harmonizing_lrtp(
        df,
        project_name_col="proj_title",
        project_description_col="subcompone",
        project_cost_col="total_project_cost",
        location_col="geometry",
        county_col="county",
        city_col="",
        project_year_col="open_year",
        data_source="MTC",
        lead_agency="",
        note_cols=[
            "plan_strategy",
            "strategy",
            "layer_name",
            "mode_detai",
            "investment",
        ],
        cost_in_millions=False,
    )

    return df

def sacog_lrtp():

    df = to_snakecase(pd.read_excel(f"{LRTP_GCS}SACOG.xlsx"))

    # Delete embedded headers
    df = delete_embedded_headers(df, "description", "Description")

    # Delete columns with all nulls
    df = df.dropna(axis=1, how="all")

    # Drop duplicates
    df = df.drop_duplicates().reset_index(drop=True)

    return df

def harmonize_sacog():
    df = sacog_lrtp()
    df = harmonizing_lrtp(
        df,
        project_name_col="title",
        project_description_col="description",
        project_cost_col="total_project_cost__2018_dollars_",
        location_col="",
        county_col="county",
        city_col="",
        project_year_col="completion_timing",
        data_source="SACOG",
        lead_agency="lead_agency",
        note_cols=[
            "budget_category",
            "year_of_expenditure_cost_for_planned_projects",
            "status__planned,_programmed_or_project_development_only_",
        ],
        cost_in_millions=True,
    )

    return df

def sandag_lrtp():
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
    sandag.cost2020m = sandag.cost2020m * 1_000_000
    sandag = correct_project_cost(sandag, "project_name", "cost2020m")
    
    sandag = sandag.drop_duplicates().reset_index(drop=True)
    return sandag

def harmonize_sandag():
    df = sandag_lrtp()
    df = harmonizing_lrtp(
        df,
        project_name_col="project_name",
        project_description_col="description",
        project_cost_col="cost2020m",
        location_col="geometry",
        county_col="",
        city_col="",
        project_year_col="phase",
        data_source="SANDAG",
        lead_agency="",
        note_cols=[
            "category",
            "status",
            "aqc_2016_1",
            "aqc_2020_1",
            "pricmcp",
            "conncmcp",
            "layer_name",
            "corridor_i",
            "type_1",
            "existing",
            "limits",
            "description_1",
            "route",
            "routetype",
            "route_desc",
            "rp_2021_id",
            "rp_2021_id_1",
            "capital_cost___2020__millions",
        ],
        cost_in_millions=False,
    )

    return df

def sbcag_lrtp():

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
    df = delete_embedded_headers(df, "description", "Description")
    df = df.dropna(subset=["description"]).reset_index(drop=True)

    df = df.drop_duplicates(
        subset=["phase", "project_title", "phase", "total_cost__$000s_"]
    ).reset_index(drop=True)

    # Millions
    df["total_cost_millions"] = df["total_cost__$000s_"] * 1_000

    return df

def harmonize_sbcag():
    df = sbcag_lrtp()
    df = harmonizing_lrtp(
        df,
        project_name_col="project_title",
        project_description_col="description",
        project_cost_col="total_cost_millions",
        location_col="",
        county_col="",
        city_col="",
        project_year_col="completion\nyear",
        data_source="SBCAG",
        lead_agency="",
        note_cols=[
            "phase",
            "type",
            "primary_funding_source_s_",
            "year",
        ],
        cost_in_millions=True,
    )

    return df

def scrtpa_lrtp():
    df = to_snakecase(pd.read_excel(f"{LRTP_GCS}SRTA.xlsx", sheet_name="Cleaned"))

    to_keep = [
        "regional_transportation_projects",
        "short_term_total_est_cost_of_project",
        "long_term_total_est_cost_of_project",
        "project_band",
        "project_type\n_project_intent_",
        "expected_funding_sources",
    ]

    df = df[to_keep]
    df["cost"] = df.short_term_total_est_cost_of_project
    df.cost = df.cost.fillna(df.long_term_total_est_cost_of_project)

    df = df.drop_duplicates()
    return df

def harmonize_scrtpa():
    df = scrtpa_lrtp()
    df = harmonizing_lrtp(
        df,
        project_name_col="regional_transportation_projects",
        project_description_col="",
        project_cost_col="cost",
        location_col="",
        county_col="",
        city_col="",
        project_year_col="project_band",
        data_source="SCRTPA",
        lead_agency="",
        note_cols=[
            "project_type\n_project_intent_",
            "expected_funding_sources",
        ],
        cost_in_millions=True,
    )

    return df

def slocog_lrtp():
    df = to_snakecase(pd.read_excel(f"{LRTP_GCS}SLOCOG.xlsx"))

    df["total_cost"] = df._2021_cost_estimate

    # Fill nan with the other columns
    df.loc[df.total_cost.isnull(), "total_cost"] = df["_2045\ntotal_capital_cost"]
    df.loc[df.total_cost.isnull(), "total_cost"] = df.escalated_cost_estimate

    drop_cols = [
        "_2045\ntotal_capital_cost",
        "_2021_cost_estimate",
        "escalated_cost_estimate",
        "project_category",
    ]

    df = df.drop(columns=drop_cols)
    df = df.reset_index()

    # Open up gdf
    gdf_url = f"{LRTP_GCS}SLOCOG Current Projects.zip"
    with fsspec.open(gdf_url) as file:
        gdf = to_snakecase(gpd.read_file(file))
    gdf = gdf[["project_id", "geometry"]]

    # Merge -> left b/c some RTP projects have no location info
    m1 = pd.merge(
        df, gdf, how="left", left_on=["_2023_rtp_project_id"], right_on=["project_id"]
    )

    m1 = m1.set_geometry("geometry")
    m1 = m1.to_crs("EPSG:4326")
    return m1

def harmonize_slocog():
    df = slocog_lrtp()
    df = harmonizing_lrtp(
        df,
        project_name_col="project_name",
        project_description_col="project_description",
        project_cost_col="total_cost",
        location_col="",
        county_col="",
        city_col="",
        project_year_col="time_period",
        data_source="SLOCOG",
        lead_agency="",
        note_cols=["project_type", "sponsor", "community", "time_horizon"],
        cost_in_millions=True,
    )

    return df

def sjcog_lrtp():
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

    concat1.project_name = concat1.project_name.fillna("None")
    concat1 = concat1[sjcog_cols]

    return concat1

def harmonize_sjcog():
    df = sjcog_lrtp()
    df = harmonizing_lrtp(
        df,
        project_name_col="project_name",
        project_description_col="project_description",
        project_cost_col="total",
        location_col="",
        county_col="",
        city_col="",
        project_year_col="completion_date",
        data_source="SJCOG",
        lead_agency="",
        note_cols=[
            "jurisdiction",
            "facility_name_route",
            "project_limits",
            "ftip_programming",
            "nepa_approval",
            "open_to_traffic",
            "completion",
            "facility_name_route_1",
            "project_description_1",
            "milestone_years_1",
        ],
        cost_in_millions=True,
    )

    return df

def scag_lrtp():
    sheets_list = ["Table 1", "Table 2"]
    dict_df = pd.read_excel(f"{LRTP_GCS}SCAG.xlsx", sheet_name=sheets_list)

    df1 = to_snakecase(dict_df.get("Table 1"))
    # Drop columns where everything is NA
    df1 = df1.dropna(axis=1, how="all")

    df2 = to_snakecase(dict_df.get("Table 2"))
    df2 = df2.dropna(axis=1, how="all")

    # Concat
    df3 = pd.concat([df1, df2])

    df4 = delete_embedded_headers(df3, "rtp_id", "RTP ID")

    df4 = df4.dropna(
        subset=["description", "lead_agency", "project_cost__$1,000s_"]
    ).reset_index(drop=True)

    # No project titles - fill it in
    df4["project_title"] = "No Title"

    # Project cost is in thousands.
    # Multiple out
    df4["project_cost_millions"] = df4["project_cost__$1,000s_"] * 1_000
    return df4

def harmonize_scag():
    df = scag_lrtp()
    df = harmonizing_lrtp(
        df,
        project_name_col="project_title",
        project_description_col="description",
        project_cost_col="project_cost_millions",
        location_col="",
        county_col="county",
        city_col="",
        project_year_col="completion_year",
        data_source="SCAG",
        lead_agency="lead_agency",
        note_cols=["system", "route_#", "route_name", "from", "to"],
        cost_in_millions=True,
    )

    return df

def stancog_lrtp():

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

    # df["title"] = (df.location + "-" + df.description).fillna("No Title")
    
    df["title"] = "None"
    
    df = delete_embedded_headers(df, "location", "Location")

    df = df.drop_duplicates(
        subset=["location", "jurisdiction", "total_cost", "open_to_traffic"]
    ).reset_index(drop=True)
    return df

def harmonize_stancog():
    df = stancog_lrtp()
    df = harmonizing_lrtp(
        df,
        project_name_col="title",
        project_description_col="description",
        project_cost_col="total_cost",
        location_col="",
        county_col="",
        city_col="",
        project_year_col="open_to_traffic",
        data_source="STANCOG",
        lead_agency="",
        note_cols=[
            "jurisdiction",
            "location",
            "project_limits",
            "funding_source",
            "system_preserv_",
            "capacity_enhance_",
            "safety",
            "oper_",
            "complete_streets",
            "active\ntransporta_tion",
            "transit",
            "other",
        ],
        cost_in_millions=True,
    )

    return df

def tmpo_lrtp():
    tahoe_url = "https://maps.trpa.org/server/rest/services/Datadownloader_Transportation/MapServer/19/query?where=1%3D1&text=&objectIds=&time=&timeRelation=esriTimeRelationOverlaps&geometry=&geometryType=esriGeometryPoint&inSR=&spatialRel=esriSpatialRelIntersects&distance=&units=esriSRUnit_Foot&relationParam=&outFields=*+&returnGeometry=true&returnTrueCurves=false&maxAllowableOffset=&geometryPrecision=&outSR=&havingClause=&returnIdsOnly=false&returnCountOnly=false&orderByFields=&groupByFieldsForStatistics=&outStatistics=&returnZ=false&returnM=false&gdbVersion=&historicMoment=&returnDistinctValues=false&resultOffset=&resultRecordCount=&returnExtentOnly=false&sqlFormat=none&datumTransformation=&parameterValues=&rangeValues=&quantizationParameters=&featureEncoding=esriDefault&f=geojson"
    gdf = to_snakecase(gpd.read_file(tahoe_url))
    gdf.complete_year = gdf.complete_year.astype(int)

    # Filter out projects that are not yet completed
    gdf2 = gdf[gdf.complete_year > 2022].reset_index(drop=True)

    gdf2["project_year"] = (
        gdf2.start_year.astype(str) + "-" + gdf.complete_year.astype(str)
    )
    return gdf2

def harmonize_tahoe():
    df = tmpo_lrtp()
    df = harmonizing_lrtp(
        df,
        project_name_col="project_name",
        project_description_col="description",
        project_cost_col="estimated_cost",
        location_col="geometry",
        county_col="",
        city_col="",
        project_year_col="project_year",
        data_source="TMPO",
        lead_agency="implementer",
        note_cols=[
            "category",
            "funding_type",
            "plan_name",
            "label",
            "financial_status",
            "url",
            "phase",
        ],
        cost_in_millions=True,
    )

    return df

def tcag_lrtp():
    df = to_snakecase(pd.read_excel(f"{LRTP_GCS}TCAG.xlsx"))

    def completion_2035(row):
        if row.completed_by_2035_y_n == "Y":
            return "2035"

    df["completion_year"] = df.apply(lambda x: completion_2035(x), axis=1)

    df["cost_x_1,000"] = df["cost_x_1,000"].abs()
    df["cost_in_millions"] = df["cost_x_1,000"] * 1_000

    df = df.drop_duplicates()
    return df

def harmonize_tcag():
    df = tcag_lrtp()
    df = harmonizing_lrtp(
        df,
        project_name_col="project_title",
        project_description_col="project_description",
        project_cost_col="cost_in_millions",
        location_col="",
        county_col="",
        city_col="",
        project_year_col="completion_year",
        data_source="TCAG",
        lead_agency="",
        note_cols=[
            "project_category",
            "jurisdiction",
            "local",
            "regional",
            "federal",
            "state",
        ],
        cost_in_millions=True,
    )

    return df

"""
LOST
"""
def harmonize_lost():
    df = harmonization_utils.load_lost()
    df = harmonizing_lrtp(
        df,
        project_name_col="project_title",
        project_description_col="project_description",
        project_cost_col="cost__in_millions_",
        location_col="",
        county_col="county",
        city_col="city",
        project_year_col="",
        data_source="LOST",
        lead_agency="",
        note_cols=[
            "measure",
            "estimated_lost_funds",
            "estimated_federal_funds",
            "estimated_state_funds",
            "estimated_local_funds",
            "estimated_other_funds",
            "notes",
        ],
        cost_in_millions=False,
    )

    return df

"""
Stack all data from
all the MPOs
"""
def all_mpo(save_to_gcs: bool = True):

    # Load harmonized df
    ambag = harmonize_ambag()
    bcag = harmonize_bcag()
    fresno = harmonize_fresnocog()
    kcag = harmonize_kcag()
    kern = harmonize_kerncog()
    madera = harmonize_madera()
    mcagov = harmonize_mcagov()
    mtc = harmonize_mtc()
    sacog = harmonize_sacog()
    sandag = harmonize_sandag()
    sbcag = harmonize_sbcag()
    scrtpa = harmonize_scrtpa()
    sjcog = harmonize_sjcog()
    scag = harmonize_scag()
    slocog = harmonize_slocog()
    stancog = harmonize_stancog()
    tahoe = harmonize_tahoe()
    tcag = harmonize_tcag()
    
    # LOST
    lost = harmonize_lost()
    lost = lost.drop(columns = ["data_source"])
    lost["data_source"] = "LOST"

    df_list = [
        ambag,
        bcag,
        fresno,
        kern,
        kcag,
        madera,
        mcagov,
        mtc,
        sacog,
        sandag,
        sbcag,
        scrtpa,
        sjcog,
        scag,
        slocog,
        stancog,
        tahoe,
        tcag,
        lost
    ]
    df = pd.concat(df_list)

    # Clean string columns
    str_cols = [
        "project_title",
        "lead_agency",
        "project_description",
        "city",
        "county",
        "data_source",
        "notes",
    ]
    for i in str_cols:
        df[i] = df[i].str.replace("_", " ").str.strip().str.title()

    
    # Create gdf
    gdf = df[df.geometry != None].reset_index(drop=True)
    gdf = gdf.set_geometry("geometry")
    gdf.geometry = gdf.geometry.set_crs("EPSG:4326")
    gdf = gdf[gdf.geometry.geometry.is_valid].reset_index(drop=True)

    if save_to_gcs:
        df.drop(columns=["geometry"]).to_excel(
            f"{harmonization_utils.GCS_FILE_PATH}LRTP/all_LRTP_LOST.xlsx", index=False
        )
        gdf.to_file("./all_LRTP_LOST.geojson", driver="GeoJSON")

    return df, gdf