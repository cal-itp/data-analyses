import pandas as pd

from siuba import *

import warehouse_queries


def categorize_values(df, col, values_dict = {}, new_colname = None):   
    if new_colname == None:
        new_colname = col
    df = (df.assign(
            col = df[col].fillna("unknown").map(values_dict)
        ).drop(columns = col)
          .rename(columns = {"col": new_colname})
    )
    
    return df


def summarize_metric_for_operator(df, group_cols = [], 
                                  numerator="", denominator=""):
    df2 = (df.groupby(group_cols)
           .agg({
               numerator: "sum", 
               denominator: "count"
           }).reset_index()
          )
    
    df2 = df2.assign(
        pct = df2[numerator].divide(df2[denominator])
    ).rename(columns = {"pct": f"pct_{numerator}"})
           
    return df2
    

def feeds_with_full_info(stop_df, trip_df):
    # Get df that shows how many unique feeds have full stop accessibility, 
    # full trip accessibility, and both
    unique_feeds_full_info = pd.merge(
        stop_df[stop_df.pct_has_stop_accessibility==1],
        trip_df[trip_df.pct_has_trip_accessibility==1],
        on = ["calitp_itp_id", "calitp_url_number"],
        how = "inner",
        validate = "1:1"
    )
    
    full_info = {
        "stops": len(stop_df[stop_df.pct_has_stop_accessibility==1]),
        "trips": len(trip_df[trip_df.pct_has_trip_accessibility==1]),
        "both": len(unique_feeds_full_info),
    }

    combined = (pd.DataFrame.from_dict(full_info, 
                                       orient="index", columns=["value"])
        .reset_index()
        .rename(columns = {"index": "category"})
       )

    combined = (combined.assign(
            total_feeds = len(stop_df),
            pct = round(combined.value / len(stop_df), 3)
        )
    )
    
    return combined
    
    
def calculate_feed_score(stop_df, trip_df):
    GROUP_COLS = ["calitp_itp_id", "calitp_url_number"]

    df = pd.merge(stop_df, trip_df, 
                  on = GROUP_COLS,
                  how = "inner", 
                  validate = "1:1")
    
    STOP_WEIGHT = 0.5
    TRIP_WEIGHT = 0.5
    
    df = df.assign(
        feed_score = ((STOP_WEIGHT * df.pct_has_stop_accessibility) + 
                      (TRIP_WEIGHT * df.pct_has_trip_accessibility)
                     )
    )
    
    return df
 
#--------------------------------------------------------------------#
## Put it all together
#--------------------------------------------------------------------#
# https://gtfs.org/reference/static/#stopstxt
# 0 is unknown; 1 is accessible; 2 is not accessible

# https://gtfs.org/reference/static/#tripstxt
# 0 is unknown; 1 is accessible; 2 is not accessible
def get_accessible_feed_data():
    STOPS_VALUES_DICT = {
        "unknown": 0,
        "0": 0, 
        "1": 1,
        "2": 1,
    }

    GROUP_COLS = ["calitp_itp_id", "calitp_url_number"]

    stops = warehouse_queries.stops >> collect()
    stops = categorize_values(stops, "wheelchair_boarding", 
                               values_dict = STOPS_VALUES_DICT, 
                               new_colname = "has_stop_accessibility")
    stops = summarize_metric_for_operator(stops, 
                                  group_cols = GROUP_COLS, 
                                  denominator = "stop_id", 
                                  numerator = "has_stop_accessibility")
    
    TRIPS_VALUES_DICT = {
        "unknown": 0,
        "0": 0, 
        "1": 1,
        "2": 1,
    }

    trips = warehouse_queries.trips >> collect()
    trips = categorize_values(trips, "wheelchair_accessible", 
                               values_dict = TRIPS_VALUES_DICT, 
                               new_colname = "has_trip_accessibility")
    trips = summarize_metric_for_operator(trips, 
                                  group_cols = GROUP_COLS, 
                                  denominator = "trip_id", 
                                  numerator = "has_trip_accessibility")

    combined = feeds_with_full_info(stops, trips)
    df = calculate_feed_score(stops, trips)

    return stops, trips, combined, df