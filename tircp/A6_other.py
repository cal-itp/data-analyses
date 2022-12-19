import pandas as pd
from calitp import *
import A1_data_prep

import altair as alt
from shared_utils import altair_utils
from shared_utils import calitp_color_palette as cp
from shared_utils import styleguide

import re
from nltk import ngrams
from nltk.corpus import stopwords

"""
Lists
"""
# Adjectives to delete
description_words_to_delete = [
    "per day",
    "electric",
    "new",
    "additional",
    "peak",
    "articulated",
    "hydrogen fuel cell",
    "hydrogen fueling",
    "battery electric",
    "zero emission",
    "hydrogen fuel cell electric",
    "express",
    "zeroemission",
    "inductive charging",
    "microtransit",
    "fuel efficient",
    "brt",
    "commuter",
    "feeder",
    "coach style",
    "cng",
    "utdc",
    "micro transit",
    "low floor",
    "battery",
    "hybrid",
    "diesel",
    "high speed", 
    "zenith",
    "low emission",
    "passenger shuttle",
    "replacement",
    "transit"

]

# List of vehicles 
bus_list = ["buses","busses", "vans", "van"]

lrv_list = [
    "light rail",
    "light rail vehicles",
    "lrv",
    "lrvs",
]
other_vehicles_list = [
    "ferry",
    "vessels",
    "trolley",
    "car",
    "trains",
    "vehicles",
    "rail",
    "vessels",
    "locomotives",
    "vehicles",
    "emus",
    "trolleys",
]

other_list = [
    "turnouts",
    "routes",
    "station",
    "parking spaces",
    "stations",
    "transit station",
    "facility",
    "locations",
]


"""
Functions
"""
# Turn value counts into a dataframe
def value_counts_df(df, col_of_interest):
    df = (
    df[col_of_interest]
    .value_counts()
    .to_frame()
    .reset_index()
    )
    return df 

# Place project status all on one row & Remove duplicate statuses
def summarize_rows(df, col_to_group: str, col_to_summarize: str):
    df = (df
          .groupby(col_to_group)[col_to_summarize]
          .apply(",".join)
          .reset_index())

    df[col_to_summarize] = (
        df[col_to_summarize]
        .apply(lambda x: ", ".join(set([y.strip() for y in x.split(",")])))
        .str.strip()
    )
    return df

# Simplify descriptions to make it easier to find metrics 
def prep_descriptions_for_extraction(
    df, description_column: str, new_column: str, unwanted_words: list
):
    """This function simplifies descriptions by lowering the strings, replacing hyphens, 
      and removing certain words such as stop words to help with tallying
      up metrics such as # of buses purchased.

    Args:
        df: the dataframe
        description_column (str): the original column.
        new_column (str): input simplified descriptions into a new column.
        unwanted_words (list): remove certain adjectives out. 
    Returns:
        The dataframe with a new column that lends itself more easily to string manipulating.
    """
    # Lowercase.
    df[new_column] = df[description_column].str.lower()

    # Remove punctation.
    df[new_column] = (df[new_column]
                      .str.replace("-", " ", regex=True)
                      .str.replace("("," ", regex=True)
                      .str.replace(")"," ", regex=True)
                      .str.replace("."," ", regex=True)
                      .str.strip()
                     )

    # Join stop word with unwanted words
    unwanted_words.extend(stopwords.words("english"))
    
    # Remove all descriptions of buses in terms of foot.
    # 30 foot bus/40 ft buses/etc 
    df[new_column] = df[new_column].str.replace(r'[0-9]\w* (foot|ft)','', regex= True)
    
    # Place all words to remove in one huge blob
    pat = r"\b(?:{})\b".format("|".join(unwanted_words))
    
    # Replace all those words with nothing
    df[new_column] = df[new_column].str.replace(pat, "", regex=True)
    
    # Replace numbers to digits.
    df[new_column] = (df[new_column]
    .str.replace("two", "2", regex=True)
    .str.replace("three", "3", regex=True)
    .str.replace("four", "4", regex=True)
    .str.replace("five", "5", regex=True)
    .str.replace("six", "6", regex=True)
    .str.replace("seven", "7", regex=True)
    .str.replace("eight", "8", regex=True)
    .str.replace("nine", "9", regex=True))

    # Replace one
    # https://stackoverflow.com/questions/53962844/applying-regex-across-entire-column-of-a-dataframe
    df[new_column] = [
            re.sub(r"\bone\b", "1", str(x)) for x in df[new_column]
        ]
        # Replace ten
    df[new_column] = [
            re.sub(r"\bten\b", "10", str(x)) for x in df[new_column]
        ]

    # Replace a with 1. 
    df[new_column] = [
            re.sub(r"\ba\b", "1", str(x)) for x in df[new_column]
        ]
    
    # Remove all four digit numbers - usually those are years
    df[new_column] = df[new_column].str.replace(r'(?<!\d)\d{4}(?!\d)', '', regex=True)
    
    return df

def extract_totals_one_category(df, description_column: str, new_col: str, keywords: list):
    """Extracts digits before a word in keywords list.
    Args:
        df: the dataframe
        description_column (str): the column that holds a description.
        new_column (str): name of the new column that holds results.
        keywords (list): all the keywords you want such as ['buses','vans','cutaway']
    Returns:
        A new column with all the digits before a certain keyword.
        Ex: a project's description states "purchased 3 vans and bought 5 buses." 
        3 and 5 will be returned.
    """
    # Delinate items in keywords list using |
    keywords = f"{'|'.join(keywords)}"

    # Extract digits that appear before keywords
    df[new_col] = df[description_column].str.findall(rf"((?!,)[0-9]+\.*[0-9]* *(?={keywords}))")

    return df

def grand_totals(df, original_column: str):
    """After using the extract_totals_one_category, the results
    are nested in a list. This function unnests the list and sums it up. 
    Args:
        df: the dataframe
        original_column (str): the original column that contains grand total. 
    Returns:
        The total. Example: a project has [3,5] listed in the column "buses_procured". This function will
        return 8 under "total_buses_procured"
    Help:
        https://practicaldatascience.co.uk/data-science/how-to-split-a-pandas-column-string-or-list-into-separate-columns
    """
    # Take  values that are nested in a list. Split each value that's separated by a
    # comma, fill in zeroes, add in prefixes, and place everything into a new df.
    df_total = (
        pd.DataFrame(df[original_column].tolist())
        .fillna(0)
        .add_prefix(f"{original_column}_")
        .astype("int64")
    )

    # Coerce into numeric
    # df_total = df_total.apply(pd.to_numeric, errors = 'coerce').fillna(0)

    # Sum up all columns to get a grand total.
    df_total[f"total_{original_column}"] = df_total.sum(axis=1)

    # Drop other columns except the Grand Total one
    df_total = df_total[[f"total_{original_column}"]]

    # Join result with original dataframe
    df = df.join(df_total)

    return df

def total_procurement_estimates(df, description_column: str, keywords_list: list, new_columns: list):
    
    """Extracts digits before certain keywords and sum them up to get grand total estimates.
    Args:
        df: the dataframe
        description_column (str): column with project description.
        keywords_list (list):  all the keywords you want. 
        new_columns (list): strings - what you your new columns to be named.
    Returns:
        A dataframe with totals based on your keywords. 
        Ex: My keywords are ['buses','lrvs','ferries']. This function will return my
        df with "total_bus", "total_lrvs", and "total_ferries" columns which 
        contain the sum of however many X were purchased per row/project.
    """
    # Loop through all the keyword lists to extract numbers. Place values into columns
    # based on values in new_columns list.
    for i in range(0, len(keywords_list)):
        df = extract_totals_one_category(
            df, description_column, f"{new_columns[i]}", keywords_list[i]
        )

    # The values are all nested into a list. Extract them and get the grand total
    for i in new_columns:
        df = grand_totals(df, i)

    # Drop the old columns, retain only totals.
    df = df.drop(columns=new_columns)

    return df

def project_description_search(df, description_column: str, keywords: list):
    """Search through project description column for certain keywords to 
    filter/categorize projects.
    Args:
        df: the dataframe
        description_column (str): the column that holds a description.
        keywords (list): all the keywords you want.
    Returns:
        A new column that flags whether keywords appeared in the description column
    """
    # Make sure everything in the keywords list is lower case
    keywords = [x.lower() for x in keywords]
    
     # Lower case description
    df[description_column] = df[description_column].str.lower()
    
    # Delinate items in keywords list using |
    keywords = f"({'|'.join(keywords)})"
    
    # New column that captures whether or not the keyword appears
    df["keywords_flag_column"] = (
    df[description_column]
    .str.extract(keywords,
        expand=False,
    )
    .fillna("Keyword not Found"))
    
    # Delete rows that don't match keywords
    df = df.loc[df["keywords_flag_column"] != "Keyword not Found"].reset_index()

    return df

"""
Old
Functions
"""
# Grabs an estimate of the number of ZEV purchased 
# in a column and tags what type of ZEV was purchased
def grab_zev_count(df, description_col: str):

    # Change the description to lower case
    # so string search will be more accurate
    df[description_col] = df[description_col].str.lower()

    # Some numbers are spelled out: replace them
    # Replace numbers that are written out into integers
    df[description_col] = (
        df[description_col]
        .str.replace("two", "2")
        .str.replace("three", "3")
        .str.replace("four", "4")
        .str.replace("five", "5")
        .str.replace("six", "6")
        .str.replace("seven", "7")
        .str.replace("eight", "8")
        .str.replace("nine", "9")
        .str.replace("eleven", "11")
        .str.replace("fifteen", "15")
        .str.replace("twenty", "20")
    )

    # Extract numbers from description into a new column
    # cast as float, fill in NA with 0
    df["number_of_zev"] = (
        df[description_col].str.extract("(\d+)").astype("float64").fillna(0)
    )

    # Tag whether the ZEV is a LRV/bus/other into a new column
    # Other includes trolleys, ferries, the general 'vehicles', and more
    df["lrv_or_bus"] = (
        df[description_col]
        .str.extract(
            "(lrv|bus|buses|light rail vehicle|coach|rail)",
            expand=False,
        )
        .fillna("other")
    )

    # Replace values to create broader categories
    df["lrv_or_bus"] = df["lrv_or_bus"].replace(
        {"lrv": "light rail vehicle", "coach": "bus", "rail": "light rail vehicle"}
    )

    return df

"""
Summary table for ZEV
"""
def zev_summary(
    df_zev,
    df_all_projects,
    group_by_cols: list,
    sum_cols: list,
    count_cols: list,
    monetary_cols: list,
):
    # Group by
    zev_summary = df_zev.groupby(group_by_cols).agg(
        {**{e: "sum" for e in sum_cols}, **{e: "count" for e in count_cols}}
    )

    zev_summary = zev_summary.reset_index()

    # Aggregate the original dataframe with ALL projects, even non ZEV in grant program
    all_projects = (
        df_all_projects.groupby(group_by_cols)
        .agg({**{e: "count" for e in count_cols}})
        .reset_index()
    )

    # Merge the summaries together to calculate % of zev projects out of total projects
    m1 = pd.merge(zev_summary, all_projects, how="inner", on=group_by_cols)

    # Get grand totals
    m1 = m1.append(m1.sum(numeric_only=True), ignore_index=True)

    # Format to currency
    m1 = A1_data_prep.currency_format(m1, monetary_cols)

    # Clean cols
    m1 = A1_data_prep.clean_up_columns(m1)

    return m1

'''
Charts Functions
'''
#Labels for charts 
def labeling(word):
    # Add specific use cases where it's not just first letter capitalized
    LABEL_DICT = { "prepared_y": "Year",
              "dist": "District",
              "nunique":"Number of Unique",
              "project_no": "Project Number"}
    
    if (word == "mpo") or (word == "rtpa"):
        word = word.upper()
    elif word in LABEL_DICT.keys():
        word = LABEL_DICT[word]
    else:
        word = word.replace('unique_', "Number of Unique ").title()
        word = word.replace('_', ' ').title()
    
    return word

# Bar chart with interactive tooltip: x_col and y_col will show up
def basic_bar_chart(df, x_col, y_col, colorcol, chart_title=''):
    if chart_title == "":
        chart_title = (f"{labeling(x_col)} by {labeling(y_col)}")
    chart = (alt.Chart(df)
             .mark_bar()
             .encode(
                 x=alt.X(x_col, title=labeling(x_col), ),
                 y=alt.Y(y_col, title=labeling(y_col),sort=('-x')),
                 color = alt.Color(colorcol, 
                                  scale=alt.Scale(
                                      range=cp.CALITP_CATEGORY_BRIGHT_COLORS),
                                      legend=alt.Legend(title=(labeling(colorcol)))
                                  ),
                tooltip = [x_col, y_col])
             .properties( 
                       title=chart_title)
    )

    chart=styleguide.preset_chart_config(chart)
    return chart


