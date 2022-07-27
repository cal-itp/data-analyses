"""
Utils for RT Schedule
"""

# Get the data for Scheduled Trips and RT Trips  
def find_ran_trips(itp_id, analysis_date):
    gtfs_daily = (
        tbl.views.gtfs_schedule_fact_daily_trips()
        >> filter(_.calitp_itp_id == itp_id)
        >> filter(_.service_date == analysis_date)
        >> filter(_.is_in_service == True)
        >> collect()
    )
    rt = query_sql(
        f"""
        SELECT * FROM `cal-itp-data-infra-staging.nla_views.test_rt_trips`
        WHERE date = '{analysis_date}'
        """
    )
    
    rt['str_len'] = rt.trip_id.str.len()
    
    #joinging 
    join = (pd.merge(gtfs_daily, rt, how='outer', on='trip_id', indicator='have_rt'))
    
    return join

#
def get_pct_ran(join, date):
    ## eventually generate a dataframe with the columns (dates and pct_ran)
    day_pct_ran = {}
    day_pct_ran['date'] = date
    day_pct_ran['pct_trips_ran'] = ((len(join>>filter(_.have_rt=='both')))/(len(join)))
    
    pct_ran = pd.DataFrame([day_pct_ran])

    return pct_ran

def get_pct_ran_df():
    pcts = []
    for date in date_list: 
        sched_rt_df = (find_ran_trips(itp_id, date))
        pct_ran = (get_pct_ran(sched_rt_df, date))
        pcts.append(pct_ran)                                                    
        #code help from: https://stackoverflow.com/questions/28669482/appending-pandas-dataframes-generated-in-a-for-loop
    pcts = pd.concat(pcts)
    return pd.DataFrame(pcts)
