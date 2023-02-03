import pandas as pd
from pandas.tseries.offsets import DateOffset
from workalendar.usa import california

cal = california.California()

def grab_ca_holidays(start_year: int, end_year: int) -> list:
    """
    Grab all of CA's holidays given a start / end year.
    Each year's holidays is returned as a dictionary. 
    Compile and save as a list to make into a pd.DataFrame later.
    """
    all_holidays = []
    
    for year in range(start_year, end_year + 1):
        holidays_in_year = cal.holidays(year)
        all_holidays = [*all_holidays, *holidays_in_year]
    
    return all_holidays


def weekend_holidays_observed_on_weekday(
    df: pd.DataFrame
) -> pd.DataFrame:
    """
    Add in an observed_date column.
    If holiday falls on Saturday, it's observed on Friday
    If holiday falls on Sunday, it's observed on Monday 
    """
    df = df.assign(
        date = pd.to_datetime(df.date),
        day_of_week = pd.to_datetime(df.date).dt.dayofweek,
        holiday = 1
    )
       
    df = df.assign(
        observed_date = df.apply(
            lambda x: x.date + DateOffset(days = -1) if x.day_of_week == 5
            else x.date + DateOffset(days = 1) if x.day_of_week == 6 
            else x.date, axis=1)
    ).drop(columns = "day_of_week")
    
    return df


if __name__=="__main__":
    # Get CA holidays 
    START_YEAR = 2015
    END_YEAR = 2050
    
    CA_HOLIDAYS = grab_ca_holidays(START_YEAR, END_YEAR)
    
    # Convert list to dataframe
    holidays = (pd.DataFrame.from_records(zip(*CA_HOLIDAYS)).T
            .rename(columns = {0: "date", 1: "holiday_name"})
           )
    
    # Add on observed_date
    holidays = weekend_holidays_observed_on_weekday(holidays)
    
    holidays.to_parquet("./holidays.parquet")

