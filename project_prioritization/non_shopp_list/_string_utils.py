import fuzzywuzzy
import _utils
from fuzzywuzzy import process

def replace_matches_set_ratio(df, column, new_col_name, string_to_match, min_ratio):
    # Get a list of unique strings
    strings = df[column].unique()

    # Get the top 10 closest matches to our input string
    matches = fuzzywuzzy.process.extract(
        string_to_match, strings, limit=10, scorer=fuzzywuzzy.fuzz.token_set_ratio
    )

    # Only get matches with a  min ratio
    close_matches = [matches[0] for matches in matches if matches[1] > min_ratio]

    # Get the rows of all the close matches in our dataframe
    rows_with_matches = df[column].isin(close_matches)

    # replace all rows with close matches with the input matches
    df.loc[rows_with_matches, new_col_name] = string_to_match
    
    
def basic_cleaning(df, agency_col: str,
                   project_name_col:str,
                   project_id_col: str, 
                   project_desc_col:str,
                   data:str):
    
    df = _utils.organization_cleaning(df, agency_col)
    
    # Remove all punctation, lowercase, and strip whitespaces from 
    # project titles & descriptions. Count number of strings.
    for i in [project_name_col, project_desc_col]:
        df[i] = (df[i].str.lower().str.replace('[^\w\s]','').str.strip())
        df[f"{i}_count"] = df[i].str.count('\w+')
                 
    # Some project names contain the year. Remove anything after 20..
    df[project_name_col] = df[project_name_col].str.split("20").str[0]
    
    # Project ID, remove all commas and lowercase if there are strings
    df[project_id_col] = (df[project_id_col].str.replace("'", "").str.lower().str.strip())
    
    # Get rid of | in object cols
    # https://stackoverflow.com/questions/68152902/extracting-only-object-type-columns-in-a-separate-list-from-a-data-frame-in-pand
    string_cols = df.select_dtypes(include=['object']).columns.to_list()
    try:
        for i in string_cols:
            df[i] = df[i].str.replace("|", "")
    except:
        pass
    
    # Try to extract titles from popups
    try:
        df["popup"] = df['popup'].str.split("<br  />").str[1].str.split("20").str[0].str.lower().str.strip().str.replace('[^\w\s]','')
    except:
        pass
 
    return df