'''
This python file includes functions that can be used to read in the 
original IIJA data as well as other IIJA data. There are also functions
that analyze string columns to get word counts and word analyses. 

If using the get_list_of_words function, remove the comment out hashtag from the import nltk and re in this script 
AND run a `! pip install nltk` in the first cell of your notebook
'''
import pandas as pd
from siuba import *
from calitp_data_analysis.sql import to_snakecase
import _script_utils
GCS_FILE_PATH  = 'gs://calitp-analytics-data/data-analyses/dla/dla-iija'

# import nltk
# from nltk.corpus import stopwords
# from nltk.tokenize import word_tokenize, sent_tokenize
# import re
def read_data_all():
    proj = to_snakecase(pd.read_excel(f"{GCS_FILE_PATH}/CopyofFMIS_Projects_Universe_IIJA_Reporting_4.xls", 
                           # sheet_name='FMIS 5 Projects  ', header=[3]
                           sheet_name='IIJA',
                           # sheet_name='FMIS 5 Projects  ',
                           ))
    proj.drop(columns =['unnamed:_0', 'unnamed:_13', 'unnamed:_14'], axis=1, inplace=True)
    proj = proj.dropna(how='all') 
    proj['summary_recipient_defined_text_field_1_value'] = proj['summary_recipient_defined_text_field_1_value'].fillna(value='None')
    
    # new_codes = to_snakecase(pd.read_excel(f"{GCS_FILE_PATH}/FY21-22ProgramCodesAsOf5-25-2022.v2.xlsx"))
    # code_map = dict(new_codes[['iija_program_code', 'new_description']].values)
    # proj['program_code_description'] = proj.program_code.map(code_map)
    
    return proj


'''
Program Code
Functions
'''
# def update_program_code_list():
    
#     ## read in the program codes
#     updated_codes = to_snakecase(pd.read_excel(f"{GCS_FILE_PATH}/program_codes/FY21-22ProgramCodesAsOf5-25-2022.v2_expanded090823.xlsx"))
#     updated_codes = updated_codes>>select(_.iija_program_code, _.new_description)
#     original_codes = to_snakecase(pd.read_excel(f"{GCS_FILE_PATH}/program_codes/Copy of lst_IIJA_Code_20230908.xlsx"))
#     original_codes = original_codes>>select(_.iija_program_code, _.description, _.program_name)
    
#     program_codes = pd.merge(updated_codes, original_codes, on='iija_program_code', how = 'outer', indicator=True)
#     program_codes['new_description'] = program_codes['new_description'].str.strip()

#     program_codes.new_description.fillna(program_codes['description'], inplace=True)
    
#     program_codes = program_codes.drop(columns={'description' , '_merge'})
    
#     return program_codes 

def update_program_code_list2():
    updated_codes = to_snakecase(pd.read_excel(f"{GCS_FILE_PATH}/program_codes/FY21-22ProgramCodesAsOf5-25-2022.v2_expanded090823.xlsx"))
    updated_codes = updated_codes>>select(_.iija_program_code, _.new_description)
    original_codes = to_snakecase(pd.read_excel(f"{GCS_FILE_PATH}/program_codes/Copy of lst_IIJA_Code_20230908.xlsx"))
    original_codes = original_codes>>select(_.iija_program_code, _.description, _.program_name)
    
    program_codes = pd.merge(updated_codes, original_codes, on='iija_program_code', how = 'outer', indicator=True)
    program_codes['new_description'] = program_codes['new_description'].str.strip()

    program_codes.new_description.fillna(program_codes['description'], inplace=True)
    
    program_codes = program_codes.drop(columns={'description' , '_merge'})
    
    def add_program_to_row(row):
        if 'Program' not in row['program_name']:
            return row['program_name'] + ' Program'
        else:
            return row['program_name']
        
    program_codes['program_name'] = program_codes.apply(add_program_to_row, axis=1)
    
    return program_codes

def add_program_to_row(row):
    if "Program" not in row["program_name"]:
        return row["program_name"] + " Program"
    else:
        return row["program_name"]

def load_program_codes_og() -> pd.DataFrame:
    df = to_snakecase(
        pd.read_excel(
            f"{GCS_FILE_PATH}/program_codes/Copy of lst_IIJA_Code_20230908.xlsx"
        )
    )[["iija_program_code", "description", "program_name"]]
    return df

def load_program_codes_sept_2023() -> pd.DataFrame:
    df = to_snakecase(
        pd.read_excel(
            f"{GCS_FILE_PATH}/program_codes/FY21-22ProgramCodesAsOf5-25-2022.v2_expanded090823.xlsx"
        )
    )[["iija_program_code", "new_description"]]
    return df

def load_program_codes_jan_2025() -> pd.DataFrame:
    df = to_snakecase(
        pd.read_excel(f"{GCS_FILE_PATH}/program_codes/Ycodes_01.2025.xlsx")
    )[["program_code", "short_name", "program_code_description", "funding_type_code"]]

    df = df.rename(
        columns={
            "program_code": "iija_program_code",
        }
    )
    df.short_name = df.short_name.str.title()
    return df

def update_program_code_list_2025():
    """
    On January 2025, we received a new list of updated codes.
    Merge this new list with codes received originally and in
    September 2023.
    """
    # Load original codes
    original_codes_df = load_program_codes_og()

    # Load September 2023 codes
    program_codes_sept_2023 = load_program_codes_sept_2023()

    # Merge original + September first
    m1 = pd.merge(
        program_codes_sept_2023,
        original_codes_df,
        on="iija_program_code",
        how="outer",
        indicator=True,
    )

    # Clean up description
    m1["new_description"] = (
        m1["new_description"].str.strip().fillna(m1.description)
    )

    # Delete unnecessary columns
    m1 = m1.drop(columns={"description", "_merge"})

    # Load January 2025 code
    program_codes_jan_2025 = load_program_codes_jan_2025()

    # Merge m1 with program codes from January 2025.
    m2 = pd.merge(
        program_codes_jan_2025,
        m1,
        on="iija_program_code",
        how="outer",
        indicator=True,
    )
    # Update descriptions
    m2["2025_description"] = (
        m2["program_code_description"].str.strip().fillna(m2.new_description)
    )

    # Update program names
    m2["2025_program_name"] = m2.program_name.fillna(m2.short_name)

    # Delete outdated columns
    m2 = m2.drop(
        columns=[
            "short_name",
            "program_name",
            "program_code_description",
            "new_description",
            "_merge",
        ]
    )

    # Rename to match original sheet
    m2 = m2.rename(
        columns={
            "2025_description": "new_description",
            "2025_program_name": "program_name",
        }
    )

    # Add program to another program names without the string "program"
    m2["program_name"] = m2.apply(add_program_to_row, axis=1)
    return m2

def add_new_codes(df):
    """
    Function to add the updated program codes to the data
    """
    #new_codes = to_snakecase(pd.read_excel(f"{GCS_FILE_PATH}/FY21-22ProgramCodesAsOf5-25-2022.v2.xlsx"))
    #code_map = dict(new_codes[['iija_program_code', 'new_description']].values)

    ## adding updated program codes 05/11/23
    #new_codes = update_program_code_list2()
    
    ## adding updated program codes 1/30/25
    new_codes = update_program_code_list_2025
    code_map = dict(new_codes[['iija_program_code', 'program_name']].values)

    df['program_code_description'] = df.program_code.map(code_map)
    df['summary_recipient_defined_text_field_1_value'] = df['summary_recipient_defined_text_field_1_value'].astype(str)
    
    # Amanda: January 2025, notified this should be called emergency supplement funding
    #df.loc[df.program_code =='ER01', 'program_code_description'] = 'Emergency Relieve Funding'
    #df.loc[df.program_code =='ER03', 'program_code_description'] = 'Emergency Relieve Funding'
    
    return df


# Function that changes "Congressional District 2" to "2"
def change_col_to_integer(df, col):
    
    df[col] = df[col].str.split(' ').str[-1]
    
    return df
 

#for use in the following function identify_agency
def add_name_from_locode(df, df_locode_extract_col):
    #read in locode sheet
    locodes = to_snakecase(pd.read_excel(f"gs://calitp-analytics-data/data-analyses/dla/e-76Obligated/locodes_updated7122021.xlsx"))
    
    #extract locode, make sure it i has the same spots
    df['locode'] = df[df_locode_extract_col].apply(lambda x: x[1:5])
    #make sure locode column is numeric
    df['locode'] = pd.to_numeric(df['locode'], errors='coerce')
    
    #merge
    df_all = (pd.merge(df, locodes, left_on='locode', right_on='agency_locode', how='left'))
    
    df_all = df_all.rename(columns={'agency_name':'implementing_agency',
                                   'locode':'implementing_agency_locode'})
    
    #if we use other locode list then drop these columns
    df_all.drop(columns =['active_e76s______7_12_2021_', 'mpo_locode_fads', 'agency_locode'], axis=1, inplace=True)
        
    return df_all


# def tokenize(texts):
#     return [nltk.tokenize.word_tokenize(t) for t in texts]


# def get_list_of_words(df, col):
#     nltk.download('stopwords')
#     nltk.download('punkt')
    
#     #get just the one col
#     column = df[[col]]
#     #remove single-dimensional entries from the shape of an array
#     col_text = column.squeeze()
#     # get list of words
#     text_list = col_text.tolist()
#     #join list of words 
#     text_list = ' '.join(text_list).lower()
    
#     # remove punctuation 
#     text_list = re.sub(r'[^\w\s]','',text_list)
#     swords = [re.sub(r"[^A-z\s]", "", sword) for sword in stopwords.words('english')]
#     # remove stopwords
#     clean_text_list = [word for word in word_tokenize(text_list.lower()) if word not in swords] 
#     # turn into a dataframe
#     clean_text_list = pd.DataFrame(np.array(clean_text_list))

#     return clean_text_list