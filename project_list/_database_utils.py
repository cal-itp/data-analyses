def to_snakecase(df):
    df.columns = df.columns.str.lower().str.replace(' ','_')
    return df

# Tag whether something is funded by state/federal/both
def is_state_funds(row):
    if row.total_state_funds > 0:
        return "Yes"
    else:
        return "No"
    
def is_fed_funds(row):
    if row.total_federal_funds > 0:
        return "Yes"
    else:
        return "No"
    
def is_local_funds(row):
    if row.total_local_funds > 0:
        return "Yes"
    else:
        return "No"