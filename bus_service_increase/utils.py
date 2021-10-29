"""
Utility functions
"""
import gcsfs
import pandas as pd
import datetime as dt
from calendar import THURSDAY, SATURDAY, SUNDAY

GCS_PROJECT = "cal-itp-data-infra"
BUCKET_NAME = "calitp-analytics-data"
BUCKET_DIR = "data-analyses/bus_service_increase"
GCS_FILE_PATH = f"gs://{BUCKET_NAME}/{BUCKET_DIR}/"

SQ_MI_PER_SQ_M = 3.86 * 10**-7

WGS84 = "EPSG:4326"

def import_export(DATASET_NAME, OUTPUT_FILE_NAME, GCS=True): 
    """
    DATASET_NAME: str. Name of csv dataset.
    OUTPUT_FILE_NAME: str. Name of output parquet dataset.
    """
    df = pd.read_csv(f"{DATASET_NAME}.csv")    
    
    if GCS is True:
        df.to_parquet(f"{GCS_FILE_PATH}{OUTPUT_FILE_NAME}.parquet")
    else:
        df.to_parquet(f"./{OUTPUT_FILE_NAME}.parquet")
    
        

def define_equity_groups(df, percentile_col = ["CIscoreP"], num_groups=5):
    """
    df: pandas.DataFrame
    percentile_col: list.
                    List of columns with values that are percentils, to be
                    grouped into bins.
    num_groups: integer.
                Number of bins, groups. Ex: for quartiles, num_groups=4.
                
    `pd.cut` vs `pd.qcut`: 
    https://stackoverflow.com/questions/30211923/what-is-the-difference-between-pandas-qcut-and-pandas-cut            
    """
    
    for col in percentile_col:
        new_col = f"{col}_group"
        df[new_col] = pd.cut(df[col], bins=num_groups, labels=False)

    return df


def prep_calenviroscreen(df):
    # Fix tract ID and calculate pop density
    df = df.assign(
        Tract = df.Tract.apply(lambda x: '0' + str(x)[:-2]).astype(str),
        sq_mi = df.geometry.area * SQ_MI_PER_SQ_M,
    )
    df['pop_sq_mi'] = df.Population / df.sq_mi
    
    df2 = define_equity_groups(
        df,
        percentile_col =  ["CIscoreP", "Pollution_", "PopCharP"], 
        num_groups = 3)
    
    # Rename columns
    keep_cols = [
        'Tract', 'ZIP', 'Population',
        'sq_mi', 'pop_sq_mi',
        'CIscoreP', 'Pollution_', 'PopCharP',
        'CIscoreP_group', 'Pollution__group', 'PopCharP_group',
        'County', 'City_1', 'geometry',  
    ]
    
    df3 = (df2[keep_cols]
           .rename(columns = 
                     {"CIscoreP_group": "equity_group",
                     "Pollution__group": "pollution_group",
                     "PopCharP_group": "popchar_group",
                     "City_1": "City",
                     "CIscoreP": "overall_ptile",
                     "Pollution_": "pollution_ptile",
                     "PopCharP": "popchar_ptile"}
                    )
           .sort_values(by="Tract")
           .reset_index(drop=True)
          )
    
    return df3


weekday_names = {THURSDAY: 'thurs', SATURDAY: 'sat', SUNDAY: 'sun'}
def get_recent_dates():
    '''
    Return a dict with dt.date objects for a recent thursday, saturday, and sunday.
    Useful for querying.
    '''

    two_wks_ago = dt.date.today() - dt.timedelta(days=14)
    dates = []
    for day_of_week in [THURSDAY, SATURDAY, SUNDAY]:
        offset = (two_wks_ago.weekday() - day_of_week) % 7
        last_day = two_wks_ago - dt.timedelta(days=offset)
        dates.append(last_day)
    
    return dict(zip(['thurs', 'sat', 'sun'], dates))

# def add_weekday_col(df):
#     '''
#     Add a column with a human-readable weekday.
#     ex. df.apply(add_weekday_col, axis=1)
#     '''
#     weekdays = {THURSDAY: 'thurs', SATURDAY: 'sat', SUNDAY: 'sun'}
#     df['weekday'] = weekdays[df.service_date.weekday()]
#     return df


# There are multiple feeds, with different trip_keys but same trip_ids
# Only keep calitp_url_number == 0 EXCEPT LA Metro
def include_exclude_multiple_feeds(df, id_col = "itp_id",
                                   include_ids = [182], exclude_ids = [200]):
    """
    df: pandas.DataFrame.
    id_col: str, column name for calitp_itp_id, such as "itp_id"
    include_ids: list, 
            list of itp_ids that are allowed to have multiple feeds 
            (Ex: LA Metro) 
    exclude_ids: list, list of itp_ids to drop. (Ex: MTC, regional feed)
    """
    # If there are explicit regional feeds to drop, put that in exclude_ids
    group_cols = [id_col, "trip_id", "stop_id", 
                  "calitp_url_number"]
    df2 = (df[~df[id_col].isin(exclude_ids)]
           .sort_values(group_cols)
           .drop_duplicates(subset=group_cols)
           .reset_index(drop=True)
          )
    
    print(f"# obs in original df: {len(df)}")
    print(f"# obs in new df: {len(df2)}")
    
    # There are still multiple operators here
    # But, seems like for those trip_ids, they are different values 
    # between url_number==0 vs url_number==1
    multiple_urls = list(df2[df2.calitp_url_number==1][id_col].unique())
    print(f"These operators have multiple calitp_url_number values: {multiple_urls}")    
    
    return df2


def aggregate_by_tract(gdf, group_cols, 
                       sum_cols = [], count_cols = []):
    '''
    gdf: geopandas.GeoDataFrame, on which the aggregating to tract is done
        It must include the tract's geometry column
    
    group_cols: list. List of columns to do the groupby, but exclude geometry.
    sum_cols: list. List of columns to calculate a sum with the groupby.
    count_cols: list. List of columns to calculate a count with the groupby.
    
    Returns a geopandas.GeoDataFrame.
    '''
    tract_geometry = gdf[["Tract", "geometry"]].drop_duplicates()
    df2 = gdf[group_cols].drop_duplicates().reset_index()
    
    if len(sum_cols) > 0:
        sum_df = gdf.pivot_table(index=group_cols, 
                                values=sum_cols, 
                                aggfunc="sum").reset_index()
        df2 = pd.merge(df2, sum_df,
                      on=group_cols, how="left", validate="1:1"
                     )
  
    if len(count_cols) > 0:
        count_df = gdf.pivot_table(index=group_cols, 
                                  values=count_cols, 
                                  aggfunc="count").reset_index()
        df2 = pd.merge(df2, count_df, 
                     on=group_cols, how="left", validate="1:1")
        
    # Merge tract geometry back in
    df3 = pd.merge(
        tract_geometry,
        df2,
        on = "Tract",
        how = "inner", 
        validate = "1:1"
    ).drop(columns = "index")
    
    return df3

def fix_gtfs_time(gtfs_timestring):
    '''Reformats a GTFS timestamp (which allows the hour to exceed 24 to mark service day continuity)
    to standard 24-hour time.
    '''
    split = gtfs_timestring.split(':')
    hour = int(split[0])
    if hour >= 24:
        split[0] = str(hour - 24)
        corrected = (':').join(split)
        return corrected.strip()
    else:
        return gtfs_timestring.strip()