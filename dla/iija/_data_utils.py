'''
This python file includes functions that can be used to read in the 
original IIJA data as well as other IIJA data. There are also functions
that analyze string columns to get word counts and word analyses. 

If using the get_list_of_words function, remove the comment out hashtag from the import nltk and re in this script 
AND run a `! pip install nltk` in the first cell of your notebook
'''

import numpy as np
import pandas as pd
from siuba import *

from shared_utils import geography_utils
import dla_utils

from calitp_data_analysis.sql import to_snakecase



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
    
    new_codes = to_snakecase(pd.read_excel(f"{GCS_FILE_PATH}/FY21-22ProgramCodesAsOf5-25-2022.v2.xlsx"))
    code_map = dict(new_codes[['iija_program_code', 'new_description']].values)
    proj['program_code_description'] = proj.program_code.map(code_map)
    
    return proj


## Function to add the updated program codes to the data
def add_new_codes(df):
    new_codes = to_snakecase(pd.read_excel(f"{GCS_FILE_PATH}/FY21-22ProgramCodesAsOf5-25-2022.v2.xlsx"))
    code_map = dict(new_codes[['iija_program_code', 'new_description']].values)
    
    df['program_code_description'] = df.program_code.map(code_map)
    df['summary_recipient_defined_text_field_1_value'] = df['summary_recipient_defined_text_field_1_value'].astype(str)
    
    return df

# Function that changes "Congressional District 2" to "2"
def change_col_to_integer(df, col):
    
    df[col] = df[col].str.split(' ').str[-1]
    
    return df

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