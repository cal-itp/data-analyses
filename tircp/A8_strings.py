import pandas as pd
from calitp_data_analysis.sql import to_snakecase
import A1_data_prep

import re
import nltk
from nltk import ngrams
from nltk.corpus import stopwords
from nltk.tokenize import sent_tokenize, word_tokenize
# from autocorrect import Speller

"""
Lists
"""
# Adjectives to delete
description_words_to_delete = [
    "per day",
    "road",
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
    "40'",
    "school",
    "feeder",
    "coach style",
    "heavy duty",
    "microtrasit ze",
    "cng",
    "utdc",
    "micro transit",
    "low floor",
    "battery",
    "hybrid",
    "diesel",
    "high speed", 
    "long-range",
    "zenith",
    "\n",
    "low emission",
    "long range",
    "low floor",
    "passenger shuttle",
    "replacement",
    "transit",
    "passenger rev",
    "long-range",

]

# List of vehicles 
bus_list = ["buses","van"]

lrv_list = [
    "light rail",
    "light rail vehicles",
    "lrv",
]

train_list = [
    "trains",
    "rail",
    "locomotives",
]

other_vehicles_list = [
    "ferry",
    "vessels",
    "trolley",
    "vehicles",
    "emus",
    "trolleys",
]

#  "parking spaces",
other_list = [
    "turnouts",
    "routes",
    "station",
    "signals",
    "facility",
    "locations",
]

"""
Functions
"""
def simplify_descriptions(
    df, description_column: str, new_column: str, unwanted_words: list
):
    """This function simplifies descriptions by lowering the strings,
      replacing hyphens, autocorrects typos, and 
      removing certain words such as stop words

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
    
    # Replace all the words above words with nothing
    df[new_column] = df[new_column].str.replace(pat, "", regex=True)
    
    # Replace numbers to digits.
    df[new_column] = (df[new_column]
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
    
     # Replace ten
    df[new_column] = [
            re.sub(r"\btwo\b", "2", str(x)) for x in df[new_column]
        ]

    # Replace a with 1. 
    df[new_column] = [
            re.sub(r"\ba\b", "1", str(x)) for x in df[new_column]
        ]
    
    # Remove all four digit numbers - usually those are years
    df[new_column] = df[new_column].str.replace(r'(?<!\d)\d{4}(?!\d)', '', regex=True)
    
    # Correct spelling 
    # https://stackoverflow.com/questions/49364664/how-to-use-autocorrect-in-pandas-column-of-sentences
    #spell = Speller(lang='en')
    #df[new_column] = df[new_column].apply(lambda x: " ".join([spell(i) for i in x.split()]))
    return df

def extract_totals_one_category(df, description_column: str, new_col: str, keywords: list):
    """Extracts digits before a word in keywords list.
    Args:
        df: the dataframe
        description_column (str): the column that holds a project description.
        new_column (str): name of the new column that holds results.
        keywords (list): all the keywords you want such as ['buses','vans','cutaway']
    Returns:
        A new column.
        Ex: a project's description states "purchased 3 vans and bought 5 buses." 
        [3,5] will be returned in the new column. 
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
    df_total[f"total_{original_column}"] = df_total.sum(axis=1).astype(int)

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
        keywords_list (list):  list containing the lists of your keywords. 
        EX: bus and vans is in the list "buses"
        "Ferry and boats" belong in the list "water_vessels". 
        Your keywords_list would be [buses, water_vessels].
        
        new_columns (list): strings - what you your new columns to be named.
        EX: ["total_buses","total_water_vessels"]
    Returns:
        A dataframe with totals based on your keywords. 
        EX: I want to find total buses purchased. Buses can be vans/bus/buses. 
        Search through a project's desc. like "purchased 14 zero emission buses, 30 45-foot vans.' 
        This function will return "44" in the new column "total_buses". 
        
        My keywords are ['buses','lrvs','ferries']. This function will return my
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

# Natalie's function to clean and place words in a project description column
# into a list
def get_list_of_words(df, col: str):

    # get just the one col
    column = df[[col]]
    
    # Correct spelling 
    # https://stackoverflow.com/questions/49364664/how-to-use-autocorrect-in-pandas-column-of-sentences
    #spell = Speller(lang='en')
    #df[col] = df[col].apply(lambda x: " ".join([spell(i) for i in x.split()]))
    
    # remove single-dimensional entries from the shape of an array
    col_text = column.squeeze()
    # get list of words
    text_list = col_text.tolist()

    # Join all the column into one large text blob, lower text
    text_list = " ".join(text_list).lower()

    # remove punctuation
    text_list = re.sub(r"[^\w\s]", "", text_list)

    # List of stopwords
    swords = [re.sub(r"[^A-z\s]", "", sword) for sword in stopwords.words("english")]

    # Append additionally words to remove from results

    # Remove stopwords
    clean_text_list = [
        word for word in word_tokenize(text_list.lower()) if word not in swords
    ]

    return clean_text_list