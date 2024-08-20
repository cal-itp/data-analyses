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






if __name__ = "__main__":
    
    good_list, review_list = fund_request_checker_v3(file_list)
    
    cleaned_fund_request = cleaner_loop(good_list)
    
    #these functions clean the DFs in the cleaned_fund_request dict
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