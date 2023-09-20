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


def update_program_code_list():
    
    ## read in the program codes
    updated_codes = to_snakecase(pd.read_excel(f"{GCS_FILE_PATH}/program_codes/FY21-22ProgramCodesAsOf5-25-2022.v2_expanded090823.xlsx"))
    updated_codes = updated_codes>>select(_.iija_program_code, _.new_description)
    original_codes = to_snakecase(pd.read_excel(f"{GCS_FILE_PATH}/program_codes/Copy of lst_IIJA_Code_20230908.xlsx"))
    original_codes = original_codes>>select(_.iija_program_code, _.description, _.program_name)
    
    program_codes = pd.merge(updated_codes, original_codes, on='iija_program_code', how = 'outer', indicator=True)
    program_codes['new_description'] = program_codes['new_description'].str.strip()

    program_codes.new_description.fillna(program_codes['description'], inplace=True)
    
    program_codes = program_codes.drop(columns={'description' , '_merge'})
    
    return program_codes 


## Function to add the updated program codes to the data
def add_new_codes(df):
    #new_codes = to_snakecase(pd.read_excel(f"{GCS_FILE_PATH}/FY21-22ProgramCodesAsOf5-25-2022.v2.xlsx"))
    #code_map = dict(new_codes[['iija_program_code', 'new_description']].values)

    ## adding updated program codes 05/11/23
    new_codes = update_program_code_list()
    code_map = dict(new_codes[['iija_program_code', 'program_name']].values)

    df['program_code_description'] = df.program_code.map(code_map)
    df['summary_recipient_defined_text_field_1_value'] = df['summary_recipient_defined_text_field_1_value'].astype(str)
    
    df.loc[df.program_code =='ER01', 'program_code_description'] = 'Emergency Relieve Funding'
    df.loc[df.program_code =='ER03', 'program_code_description'] = 'Emergency Relieve Funding'
    
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