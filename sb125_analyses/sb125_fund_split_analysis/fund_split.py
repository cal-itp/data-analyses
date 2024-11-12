# A lot of dataframes needed to be uniquly cleaned, 

import altair as alt
import pandas as pd

GCS_PATH = "gs://calitp-analytics-data/data-analyses/sb125/fund_split/"

file_list = [
    "sierra_fund_request.xlsx",
    "slocog_fund_request.xlsx",
    "tehema_fund_request.xlsx",
    "tuolumne_fund_request.xlsx",
    "ventura_fund_request.xlsx",
    "alpine_fund_request.xlsx",
    "amador_fund_request.xlsx",
    "butte_fund_request.xlsx",
    "calaveras_fund_request.xlsx",
    "del_norte_fund_request.xlsx",
    "el_dorado_fund_request.xlsx",
    "humboldt_fund_request.xlsx",
    "kern_fund_request.xlsx",
    "kings_fund_request.xlsx",
    "la_metro_fund_request.xlsx",
    "lake_fund_request.xlsx",
    "lassen_fund_request.xlsx",
    "madera_fund_request.xlsx",
    "mariposa_fund_request.xlsx",
    "mendocino_fund_request.xlsx",
    "merced_fund_request.xlsx",
    "mtc_fund_request.xlsx",
    "nevada_fund_request.xlsx",
    "orange_fund_request.xlsx",
    "placer_fund_request.xlsx",
    "plumas_fund_request.xlsx",
    "riverside_fund_request.xlsx",
    "san_benito_fund_request.xlsx",
    "san_diego_mts_fund_request.xlsx",
    "santa_cruz_fund_request.xlsx",
    "shasta_fund_request.xlsx",
]

file_list.sort()

col_names = [
    "rtpa",
    "implementing agenc-y/-ies",
    "project",
    "fund source",
    "capital_FY23-24",
    "capital_FY24-25",
    "capital_FY25-26",
    "capital_FY26-27",
    "operating_FY23-24",
    "operating_FY24-25",
    "operating_FY25-26",
    "operating_FY26-27",
    "total",
]


def clean_fund_request(file: str) -> pd.DataFrame:
    """
    reads in the file from GCS, maps col_names list to df columns, drops all the blank rows.
    returns df.
    """
    col_names = [
        "rtpa",
        "implementing agenc-y/-ies",
        "project",
        "fund source",
        "capital_FY23-24",
        "capital_FY24-25",
        "capital_FY25-26",
        "capital_FY26-27",
        "operating_FY23-24",
        "operating_FY24-25",
        "operating_FY25-26",
        "operating_FY26-27",
        "total",
    ]

    df = pd.read_excel(f"{GCS_PATH}{file}", header=2, nrows=40, names=col_names).drop(
        columns="total"
    )
    row_drop = df["rtpa"].isin(["Grand Total", "RTPA"])
    df = df.drop(df[row_drop].index)
    df = df.dropna(how="all")
    df[["rtpa", "implementing agenc-y/-ies", "project"]] = df[
        ["rtpa", "implementing agenc-y/-ies", "project"]
    ].ffill()

    return df


def fund_request_checker_v3(file_list: list) -> tuple:
    """takes in list of fund_request excel file name. reads in each file, checks if DF has 13 columns.
    if yes, appends do good-to-go list. else, appends to needs-manual-review.
    output is a tuple of the 2 list.
    assign 2 variables to use this func.
    """
    gtg_files = []
    manual_review = []
    for file in file_list:

        df = pd.read_excel(f"{GCS_PATH}{file}", nrows=40)
        df = df.dropna(how="all")

        if len(df.columns) == 13:
            gtg_files.append(f"{file}")
        else:
            manual_review.append(f"{file}")
    return gtg_files, manual_review


def cleaner_loop(gtg_list: list) -> dict:
    """
    takes in good-to-go list from fund_request_checker.
    applies the clean_fund_request function to each item on the list, then appends to dictionary.
    key is name of the file, value is the cleaned dataframe.
    output is dictionary.
    """
    cleaned_df = {}

    for name in gtg_list:
        cleaned_df[name] = clean_fund_request(name)
    return cleaned_df


def clean_humboldt():
    cleaned_fund_request["humboldt_fund_request.xlsx"][
    ["operating_FY24-25", "operating_FY25-26", "operating_FY26-27"]
] = cleaned_fund_request["humboldt_fund_request.xlsx"][
    ["operating_FY24-25", "operating_FY25-26", "operating_FY26-27"]
].replace(
    "-", 0
)
    return


def clean_amador():
    cleaned_fund_request["amador_fund_request.xlsx"] = cleaned_fund_request[
    "amador_fund_request.xlsx"
    ][cleaned_fund_request["amador_fund_request.xlsx"]["fund source"].notna()
     ]
    return


def clean_merced():
    cleaned_fund_request["merced_fund_request.xlsx"]= cleaned_fund_request[
    "merced_fund_request.xlsx"][cleaned_fund_request["merced_fund_request.xlsx"]["project"].notna()].drop([34, 36, 37])
    
    # is there another way to update these values that doesnt include using the number index
    cleaned_fund_request["merced_fund_request.xlsx"].at[13, "fund source"] = "`5339"
    cleaned_fund_request["merced_fund_request.xlsx"].at[19, "fund source"] = "`5307"
    
    return


def clean_san_benito():
    cleaned_fund_request["san_benito_fund_request.xlsx"] = cleaned_fund_request["san_benito_fund_request.xlsx"][
    cleaned_fund_request["san_benito_fund_request.xlsx"]["fund source"].notna()]
    
    return


def clean_mts():
    cleaned_fund_request["san_diego_mts_fund_request.xlsx"] = cleaned_fund_request["san_diego_mts_fund_request.xlsx"].iloc[7:]
    
    return


def clean_sierra():
    cleaned_fund_request["sierra_fund_request.xlsx"] = cleaned_fund_request["sierra_fund_request.xlsx"].iloc[:-8]
    
    return


def clean_nevada():
    cleaned_fund_request["nevada_fund_request.xlsx"]=cleaned_fund_request["nevada_fund_request.xlsx"][
    cleaned_fund_request["nevada_fund_request.xlsx"]["fund source"].notna()]
    
    return


def clean_plumas():
    cleaned_fund_request["plumas_fund_request.xlsx"]=cleaned_fund_request["plumas_fund_request.xlsx"][cleaned_fund_request["plumas_fund_request.xlsx"][
    "fund source"].notna()]
    
    return


def clean_lassen():
    lassen = pd.read_excel(
        f"{GCS_PATH}lassen_fund_request.xlsx",
        sheet_name="D.2. Detailed Fund Request",
        skiprows=6,
        header=0,
        skipfooter=12,
    ).drop(columns=["Unnamed: 0", "Project Type", "Operator"])

    # can work with this. may be able to remove the top rows then use cleaner loop
    lassen.columns = col_names
    lassen_cleaned = lassen.drop(columns="total")
    
    return lassen_cleaned


def clean_butte():
    butte = pd.read_excel(
        f"{GCS_PATH}butte_fund_request.xlsx",
        skiprows=2,
        header=0,
        skipfooter=17,
    )

    butte_cleaned = butte.copy()

    butte_cleaned[["RTPA", "Implementing Agenc-y/-ies", "Project"]] = butte_cleaned[
        ["RTPA", "Implementing Agenc-y/-ies", "Project"]
    ].ffill()

    butte_cleaned.insert(6, "operations FY25-26", 0)
    butte_cleaned.insert(7, "operations FY26-27", 0)
    butte_cleaned.columns = [
        "rtpa",
        "implementing agenc-y/-ies",
        "project",
        "fund source",
        "operating_FY23-24",
        "operating_FY24-25",
        "operating_FY25-26",
        "operating_FY26-27",
        "capital_FY23-24",
        "capital_FY24-25",
        "capital_FY25-26",
        "capital_FY26-27",
        "total",
    ]
    butte_cleaned = butte_cleaned.drop(columns="total")

    return butte_cleaned


def clean_mtc():
    mtc = pd.read_excel(
        f"{GCS_PATH}mtc_fund_request.xlsx", skiprows=2, header=0, skipfooter=21
    ).drop(columns=["Unnamed: 13", "Unnamed: 14", "Unnamed: 15"])

    mtc_cleaned = mtc.copy()
    mtc_cleaned.columns = col_names
    mtc_cleaned = mtc_cleaned.drop(columns="total")
    
    return mtc_cleaned


def clean_orange():
    orange = pd.read_excel(
        f"{GCS_PATH}orange_fund_request.xlsx", skiprows=3, header=0, skipfooter=1
    )

    orange_cleaned = orange.copy()

    orange_cleaned.rename(columns={"Unnamed: 0": "RTPA"}, inplace=True)
    orange_cleaned["RTPA"] = "OCTA"
    orange_cleaned = orange_cleaned.drop(
        columns=["FY27-28", "FY28-29", "FY27-28.1", "FY28-29.1"]
    )

    orange_cleaned.columns = col_names
    orange_cleaned = orange_cleaned.drop(columns="total")
    
    return orange_cleaned


def clean_santa_cruz():
    santa_cruz = pd.read_excel(
        f"{GCS_PATH}santa_cruz_fund_request.xlsx", skiprows=4, header=0, skipfooter=5
    ).iloc[:, 0:13]

    santa_cruz_cleaned = santa_cruz.copy()

    santa_cruz_cleaned.columns = col_names
    santa_cruz_cleaned.drop(columns="total", inplace=True)
    
    return santa_cruz_cleaned


def clean_ventura():
    #clean TIRCP sections
    ventura_tircp_capital = pd.read_excel(
    f"{GCS_PATH}ventura_fund_request.xlsx",
    sheet_name="Project Breakdown",
    skiprows=2,
    header=0,
    skipfooter=40,
    )
    
    ventura_tircp_operating = pd.read_excel(
        f"{GCS_PATH}ventura_fund_request.xlsx",
        sheet_name="Project Breakdown",
        skiprows=51,
        header=0,
        skipfooter=1,
    )
    
    ventura_tircp_merge = ventura_tircp_capital.merge(
        ventura_tircp_operating,
        how="outer",
        on=["Implementing Agenc-y/-ies", "Project Category", "Project"],
        suffixes=["_capital", "_operating"],
    ).drop(
        columns=[
            "Year Requested",
            "Unnamed: 8_capital",
            "Unnamed: 8_operating",
            "Project Category",
        ]
    )
    
    #merging TIRCP sections
    ventura_tircp_merge["rtpa"] = "VCTC"
    ventura_tircp_merge["Fund Source"] = "TIRCP"

    ventura_col_dict = {
        "Implementing Agenc-y/-ies": "implementing agenc-y/-ies",
        "Project": "project",
        "Fund Source": "fund source",
        "FY23-24_capital": "capital_FY23-24",
        "FY24-25_capital": "capital_FY24-25",
        "FY25-26_capital": "capital_FY25-26",
        "FY26-27_capital": "capital_FY26-27",
        "FY23-24_operating": "operating_FY23-24",
        "FY24-25_operating": "operating_FY24-25",
        "FY25-26_operating": "operating_FY25-26",
        "FY26-27_operating": "operating_FY26-27",
    }

    col_order = [
        "rtpa",
        "implementing agenc-y/-ies",
        "project",
        "fund source",
        "capital_FY23-24",
        "capital_FY24-25",
        "capital_FY25-26",
        "capital_FY26-27",
        "operating_FY23-24",
        "operating_FY24-25",
        "operating_FY25-26",
        "operating_FY26-27",
    ]

    ventura_tircp_merge.rename(columns=ventura_col_dict, inplace=True)

    ventura_tircp_merge = ventura_tircp_merge[col_order]
    
    # clean zetcp sections
    ventura_zetcp_capital = pd.read_excel(
        f"{GCS_PATH}ventura_fund_request.xlsx",
        sheet_name="Project Breakdown",
        skiprows=32,
        header=0,
        skipfooter=21,
    ).drop(columns=["Unnamed: 7", "Unnamed: 8"])
    fund_dict = {
        "GGRF Y1": "ZETCP (GGRF)",
        "GGRF Y2": "ZETCP (GGRF)",
        "GGRF Y3": "ZETCP (GGRF)",
        "GGRF Y4": "ZETCP (GGRF)",
        "PTA": "ZETCP (PTA)",
    }

    ven_col = {
        "Implementing Agenc-y/-ies": "implementing agenc-y/-ies",
        "Project": "project",
        "Fund Source": "fund source",
        "FY23-24": "capital_FY23-24",
        "FY24-25": "capital_FY24-25",
        "FY25-26": "capital_FY25-26",
        "FY26-27": "capital_FY26-27",
    }

    ventura_zetcp_capital["Fund Source"] = ventura_zetcp_capital["Fund Source"].replace(
        fund_dict
    )

    ventura_zetcp_capital["rtpa"] = "VCTC"

    ventura_zetcp_capital.rename(columns=ven_col, inplace=True)

    ventura_zetcp_capital = ventura_zetcp_capital[col_order[0:8]]
    
    #final merge
    ventura_big_merge = ventura_tircp_merge.merge(
        ventura_zetcp_capital,
        how="outer",
        on=[
            "implementing agenc-y/-ies",
            "project",
            "fund source",
            "capital_FY23-24",
            "capital_FY24-25",
            "capital_FY25-26",
            "capital_FY26-27",
            "rtpa",
        ],
        suffixes=("_zetcp_cap", "_tircp"),
    )
    return ventura_big_merge


def get_kern_data(head_count: int, foot_count: int, agency: str, proj_title: str):

    kern_cols = {
        "Unnamed: 0": "fund source",
        "Unnamed: 1": "capital_FY23-24",
        "Unnamed: 2": "operating_FY23-24",
        "Unnamed: 3": "capital_FY24-25",
        "Unnamed: 4": "operating_FY24-25",
        "Unnamed: 5": "capital_FY25-26",
        "Unnamed: 6": "operating_FY25-26",
        "Unnamed: 7": "capital_FY26-27",
        "Unnamed: 8": "operating_FY26-27",
    }
    df = (
        pd.read_excel(
            f"{GCS_PATH}kern_fund_request.xlsx",
            header=head_count,
            skipfooter=foot_count,
        )
        .rename(columns=kern_cols)
        .assign(implementing_agency=agency, rtpa="Kern COG", project=proj_title)
    )
    return df


def clean_kern():
    
    col_order = [
        "rtpa",
        "implementing agenc-y/-ies",
        "project",
        "fund source",
        "capital_FY23-24",
        "capital_FY24-25",
        "capital_FY25-26",
        "capital_FY26-27",
        "operating_FY23-24",
        "operating_FY24-25",
        "operating_FY25-26",
        "operating_FY26-27",
    ]
    
    kern_cols = {
        "Unnamed: 0": "fund source",
        "Unnamed: 1": "capital_FY23-24",
        "Unnamed: 2": "operating_FY23-24",
        "Unnamed: 3": "capital_FY24-25",
        "Unnamed: 4": "operating_FY24-25",
        "Unnamed: 5": "capital_FY25-26",
        "Unnamed: 6": "operating_FY25-26",
        "Unnamed: 7": "capital_FY26-27",
        "Unnamed: 8": "operating_FY26-27",
    }

    arvin = get_kern_data(
        3,
        43,
        "arvin",
        "Purcahse and install EV mirco-grid Purcahse and install EV mirco-grid",
    )

    california_city = get_kern_data(
        7,
        39,
        "california city",
        "Purchase and construct transit building to house EV vans and solar charging stations",
    )

    delano = get_kern_data(11, 35, "delano", "Construct transit Facility")

    get = get_kern_data(
        15,
        31,
        "get",
        "Golden Empire Transit Free or near free transit fares Back up hydrogent fuel plant",
    )

    kern_transit = get_kern_data(
        19,
        27,
        "kern transit",
        "Transition to Zero-Emission Vehicles and supporting infrastructure",
    )

    mcfarland = get_kern_data(
        23,
        23,
        "mcfarland",
        "Design and construct a transit station providing a transit office , waitning area, restrooms and EV charging stations",
    )

    ridgecrest = get_kern_data(
        27,
        19,
        "ridgecrest",
        "Replacement of Cutaway Buses with Electric Vans and Construction of Bus Stop at North Norma Street and West Felspar Avenue",
    )

    shafter = get_kern_data(
        31, 15, "shafter", "bus storage, new transit vehicles, free ridership fare program"
    )

    taft = get_kern_data(
        35,
        11,
        "taft",
        "convert the City’s gasoline powered fleet of on-demand transit vehicles to plug-in electric vans compatible with the solar-powered charging infrastructure being completed now.",
    )

    tehahapi = get_kern_data(
        39,
        9,
        "tehahapi",
        "Improvements to Downtown Transit Center and Installation of EV Charging Infrastructure",
    )

    wasco = get_kern_data(
        43, 
        3, 
        "wasco", "New Transit Operating and Maintenance Facility"
    )

    kern_concat = pd.concat(
        [
            wasco,
            tehahapi,
            taft,
            shafter,
            ridgecrest,
            mcfarland,
            kern_transit,
            get,
            arvin,
            california_city,
            delano,
        ],
        ignore_index=True,
    )

    kern_clean = kern_concat[kern_concat["fund source"].isin(["TIRCP", "ZETCP"])].rename(
        columns={"implementing_agency": "implementing agenc-y/-ies"}
    )

    kern_clean = kern_clean[col_order]
    
    return kern_clean


def concat_everything():
    all_fund_requests = pd.concat(cleaned_fund_request.values(), ignore_index=True)
    
    all_fund_requests = pd.concat(
    [
        all_fund_requests,
        santa_cruz_cleaned,
        orange_cleaned,
        butte_cleaned,
        lassen_cleaned,
        ventura_cleaned,
        kern_cleaned,
        mtc_cleaned
    ],
    ignore_index=True,
    )
    
    all_fund_requests["fund source"] = all_fund_requests["fund source"].astype(str)
    
    return all_fund_requests


def fund_request_melt(df):
    id_vars = [
        "rtpa",
        "implementing agenc-y/-ies",
        "project",
        "fund source",
    ]
    val_vars = [
        "capital_FY23-24",
        "capital_FY24-25",
        "capital_FY25-26",
        "capital_FY26-27",
        "operating_FY23-24",
        "operating_FY24-25",
        "operating_FY25-26",
        "operating_FY26-27",
    ]

    melt = df.melt(
        id_vars=id_vars,
        value_vars=val_vars,
        var_name="capital/operation fy",
        value_name="fund amount",
        ignore_index=True,
    )
    
    # splitting the cap/operations columns

    melt[["project type", "fiscal year"]] = melt["capital/operation fy"].str.split(
        "_FY", expand=True
    )

    melt["fund amount"] = melt["fund amount"].fillna(0).astype("int64")
    melt["fund source"] = melt["fund source"].astype("str")
    
    return melt



if __name__ == "__main__":
    
    good_list, review_list = fund_request_checker_v3(file_list)
    
    cleaned_fund_request = cleaner_loop(good_list)
    
    #these functions clean specific values (DFs) in the cleaned_fund_request dict
    clean_humboldt()
    
    clean_amador()
    
    clean_merced()
    
    clean_san_benito()
    
    clean_mts()
    
    clean_sierra()
    
    clean_nevada()
    
    clean_plumas()
    
    #these functions clean the problem data sets
    lassen_cleaned = clean_lassen()
    
    butte_cleaned = clean_butte()
    
    mtc_cleaned = clean_mtc()
    
    orange_cleaned = clean_orange()
    
    santa_cruz_cleaned = clean_santa_cruz()
    
    ventura_cleaned = clean_ventura()
    
    kern_cleaned = clean_kern()
    
    # concat all values (DFs) from cleaned_fund_request dict to be a single DF and concat the rest of the DFs
    
    all_fund_requests = concat_everything()
    
    # SAVING TO GCS!
    all_fund_requests.to_parquet(f"{GCS_PATH}all_fund_requests_concat.parquet")

    #melt all fund requests
    all_melt = fund_request_melt(all_fund_requests)
    
    #saving to gcs
    all_melt.to_parquet(f"{GCS_PATH}all_fund_requests_melt.parquet")