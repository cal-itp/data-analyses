import pandas as pd 
def csis_clean_project(df:pd.DataFrame)->pd.DataFrame:
    df = df.fillna(df.dtypes.replace({'float64': 0.0, 'object': 'None', 'int64': 0}))
    string_cols = [
     'needpurpose',
     'proj_desc',
     'route1',
     'title']
    # Clean strings
    for i in string_cols:
        df[i] = df[i].str.title().str.lstrip().str.rstrip()
        df[i] = df[i].replace(r'\s+', ' ', regex=True)
        
    # Drop projects by ctips_id
    df2 = df.drop_duplicates(subset = ['ctips_id'])
    
    # Filter out any rows where chg_qual1==7 because those are projects that are deleted
    df2 = df2.loc[(df2.chg_qual1 != 7)]
    df2 = df2.loc[(df2.archive == 0)]
    df2 = df2.loc[(df2.document != "DSHOPP")]
    df2 = df2.loc[(df2.chg_offcl != 14)]
    df2 = df2.loc[(df2.chg_qual1 != 15)]
    df2 = df2.loc[(df2.chg_qual1 != 16)]
    df2 = df2.loc[(df2.chg_qual1 != 18)]
    df2 = df2.loc[(df2.chg_qual1 != 20)]
    df2 = df2.loc[(df2.chg_qual1 != 28)]
    return df2

def add_agencies(left_df: pd.DataFrame, right_df: pd.DataFrame, col: str) -> pd.DataFrame:
    merged_df = pd.merge(
        left_df,
        right_df,
        left_on=col,
        right_on='agencyid',
        how='left'
    )

    renamed_df = merged_df.rename(
        columns={
            'agency_name_y': f'{col}_agency',
            'agencyid_x': 'agencyid',
            'agency_name_x': 'agency_name'
        }
    )

    final_df = renamed_df.drop(columns=['agencyid_y'])

    return final_df

def add_counties(left_df: pd.DataFrame, right_df: pd.DataFrame, col: str) -> pd.DataFrame:
    merged_df = pd.merge(
        left_df,
        right_df,
        left_on=col,
        right_on='countyid',
        how='left'
    )

    renamed_df = merged_df.rename(
        columns={
            'county_name_y': f'{col}_county',
            'countyid_x': 'countyid',
            'county_name_x': 'county_name'
        }
    )

    final_df = renamed_df.drop(columns=['countyid_y',  col])

    return final_df

def calculate_state_fed_local_total_funds(df:pd.DataFrame, fund_keywords:list, total_col_name:str)->pd.DataFrame:
    selected_columns = [col for col in df.columns if any(keyword.lower() in col.lower() for keyword in fund_keywords)]
    df[total_col_name] = df[selected_columns].fillna(0).sum(axis = 1)
    return df 

def clean_political(df:pd.DataFrame, keyword_to_search:str)->pd.DataFrame:
    my_list = []
    # Append a string to the list
    my_list.append(keyword_to_search)
    
    filtered_columns = [col for col in df.columns if any(keyword.lower() in col.lower() for keyword in my_list)]
    all_cols = filtered_columns + ['ctips_id']
    df2 = df[all_cols]
    
    # Make this from wide to long
    df2 = pd.melt(df2, id_vars=['ctips_id'], value_vars=filtered_columns)
    
    # Clean up columns
    df2.variable = df2.variable.str.replace(keyword_to_search, '')
    df2 = df2.rename(columns = {'variable':keyword_to_search})
    
    # Only keep relevant values for each project
    df2 = df2.loc[df2.value == 1.0].reset_index(drop = True).drop(columns = ['value'])
    return df2