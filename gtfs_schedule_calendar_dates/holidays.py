import pandas as pd
from workalendar.usa import california

cal = california.California()

def grab_ca_holidays(start_year: int, end_year: int) -> list:
    all_holidays = []
    for year in range(start_year, end_year + 1):
        holidays_in_year = cal.holidays(year)
        all_holidays = [*all_holidays, *holidays_in_year]
    
    return all_holidays


if __name__=="__main__":
    CA_HOLIDAYS = grab_ca_holidays(2015, 2050)
    
    holidays = (pd.DataFrame.from_records(zip(*CA_HOLIDAYS)).T
            .rename(columns = {0: "date", 1: "holiday_name"})
           )

    holidays = holidays.assign(
        date = pd.to_datetime(holidays.date),
        holiday = 1
    )
    
    holidays.to_parquet("./holidays.parquet")

