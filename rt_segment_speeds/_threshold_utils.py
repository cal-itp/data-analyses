import geopandas as gpd
import numpy as np
import pandas as pd
from calitp.sql import to_snakecase


import altair as alt
from shared_utils import calitp_color_palette as cp
from shared_utils import geography_utils, rt_utils, styleguide, utils

import intake
catalog = intake.open_catalog("./catalog_threshold.yml")

"""
Lists
"""
TIME_CUTOFFS = [5, 10, 15]
SEGMENT_CUTOFFS = [
    0.1,
    0.2,
    0.3,
    0.4,
    0.5,
    0.6,
    0.7,
    0.8
]

"""
Prepare data from catalog
"""
def clean_trips():
    df = catalog.trips.read()
    subset = [
        "feed_key",
        "name",
        "route_id",
        "direction_id",
        "shape_id",
    ]

    df = df[subset]
    df = df.drop_duplicates().reset_index(drop=True)
    return df

def clean_routelines():
    df = catalog.route_lines.read()

    df = df.drop(columns=["shape_array_key"])
    
    df = (df.drop_duplicates()).reset_index(drop=True)

    # Calculate length of geometry
    df = df.assign(actual_route_length=(df.geometry.length))

    return df

def clean_longest_shape():
    df = catalog.longest_shape.read()

    df = df.rename(columns={"route_length": "longest_route_length"})

    return df

"""
Creating insights
"""
def merge_trips_routes_longest_shape():
    """
    Merge and find the shape_id's length
    versus the longest shape_id's length.
    Count segments.
    """
    trips = clean_trips()
    crosswalk = catalog.crosswalk.read()
    routelines = clean_routelines()
    longest_shape = clean_longest_shape()

    m1 = (
        trips.merge(
            crosswalk, how="inner", on=["feed_key", "route_id", "name", "direction_id"]
        )
        .merge(routelines, how="inner", on=["feed_key", "shape_id"])
        .merge(
            longest_shape.drop(columns=["geometry"]),
            how="inner",
            on=[ "feed_key","gtfs_dataset_key","direction_id","route_id","route_dir_identifier","name"],
        )
    )

    # Calculate out proportion of route length against longest.
    m1["route_length_percentage"] = (
        (m1["actual_route_length"] / m1["longest_route_length"]) * 100
    ).astype(int)

    # Count number of segments that appear in the longest shape.
    m1 = (
        m1.groupby(
            [
                "route_id",
                "name",
                "gtfs_dataset_key",
                "route_dir_identifier",
                "shape_id",
                "longest_shape_id",
                "route_length_percentage",
            ]
        )
        .agg({"segment_sequence": "count"})
        .rename(columns={"segment_sequence": "total_segments"})
        .reset_index()
    )

    return m1

def summary_stats_route_length():
    """
    Get mean, median, max, and min longest shape_id
    versus actual shape_id  of route length for every operator.
    All in one dataframe.
    """
    df = merge_trips_routes_longest_shape()

    df = (
        df.groupby(["gtfs_dataset_key", "name", "route_id", "shape_id"])
        .agg({"route_length_percentage": "max"})
        .reset_index()
    )
    
    # Get summary stats
    df = (
        df.groupby(["name", "gtfs_dataset_key"])
        .agg({"route_length_percentage": ["mean", "median", "min", "max"]})
        .reset_index()
    )
    
    # Drop index
    df.columns = df.columns.droplevel()
 
    df.columns.values[0] = "name"
    df.columns.values[1] = "gtfs_dataset_key"
    
    # Melt to long df
    df = pd.melt(df, id_vars=["name", "gtfs_dataset_key"], value_vars=["mean", "median", "min", "max"])
    df = df.rename(columns={"value": "route_length_percentage",})
    
    # Title case variable col
    df.variable = df.variable.str.title()
    
    # Round value col for axis
    df['rounded_route_length_percentage'] = ((df.route_length_percentage/100)*10).astype(int)*10
    
    # Sort values by name and mean/median
    df = df.sort_values(['name','variable']).reset_index(drop = True)
    return df

def merge_trip_diagnostics_with_total_segments():
    """
    Find trip time. Find total segments that 
    actually appear versus segments that appear
    in the longest shape
    """
    trip_diagnostics = catalog.trip_stats.read()
    
    # Load in longest shape
    segments = catalog.longest_shape.read()
    
    # Count # of segments by longest recorded shape.
    # For each route direction and operator.
    total_segments_by_shape = (
        segments.groupby(["gtfs_dataset_key", "name", "route_dir_identifier"])
        .segment_sequence.nunique()
        .reset_index()
        .rename(columns={"segment_sequence": "total_segments"})
    )
    
    df = pd.merge(
        trip_diagnostics,
        total_segments_by_shape,
        on=["gtfs_dataset_key", "route_dir_identifier"],
        how="inner",
        validate="m:1",
    )
    
    # Find the total of segments that appear vs. what 'should' appear,
    # trip time, and number of trips the operator made in total.
    df = df.assign(
        pct_vp_segments=df.num_segments_with_vp.divide(df.total_segments),
        trip_time=((df.trip_end - df.trip_start) / np.timedelta64(1, "s") / 60).astype(
            int
        ),
        total_trips=df.groupby(["gtfs_dataset_key", "name"]).trip_id.transform(
            "nunique"
        ),
    )

    return df

def summary_valid_trips_by_cutoff(df, time_cutoffs: list, segment_cutoffs: list):
    """
    Find percentage & number of trips that meet trip time 
    and percentage of segment thresholds by operators.
    """
    final = pd.DataFrame()

    for t in time_cutoffs:
        for s in segment_cutoffs:
            valid = (
                df[(df.trip_time >= t) & (df.pct_vp_segments >= s)]
                .groupby(["gtfs_dataset_key", "name", "total_trips"])
                .trip_id.nunique()
                .reset_index()
                .rename(columns={"trip_id": "n_trips"})
            )

            valid = valid.assign(
                trip_cutoff=t, segment_cutoff=s*100, cutoff=f"{t}+ min & {s*100}%+ segments"
            )

            final = pd.concat([final, valid], axis=0)

    final = final.assign(percentage_usable_trips=final.n_trips.divide(final.total_trips) * 100)

    return final

def load_dataframes(time_cutoff:list, segment_cutoffs:list):
    """
    Load all relevant manipulated dataframes
    in one go. 
    """
    #  Operator # Load in dataframe that has  route lengths
    # based on shape id and longest shape id
    route_df =  pre_clean(summary_stats_route_length())
    
    # Load in dataframe with trip times and 
    # percent of segments that actually show up vs what is recorded. 
    time_segments_df = merge_trip_diagnostics_with_total_segments()
    
    # Find thresholds number and percentage of trips 
    # after thresholds
    valid_stats_df = pre_clean(summary_valid_trips_by_cutoff(time_segments_df, time_cutoff, segment_cutoffs))

    # Filter out any operators without RT information
    routelengthlist = set(route_df.Name.unique().tolist())
    tripslist = set(valid_stats_df.Name.unique().tolist())
    operators_wo_rt = list(routelengthlist - tripslist)
    route_length = route_df.loc[~route_df.Name.isin(operators_wo_rt)].reset_index(drop = True)
    
    return route_df, time_segments_df, valid_stats_df
    
"""
Other
"""
# Preclean the dataframe before inputting it into 
# charts and graphs.
def pre_clean(df):
    df = df.round(1)
    df = clean_up_columns(df)
    return df

# Reverse snake_case
def clean_up_columns(df):
    df.columns = df.columns.str.replace("_", " ").str.strip().str.title()
    return df

"""
General Chart Functions
"""
# Format the size of a chart
def chart_size(chart: alt.Chart, chart_width: int, chart_height: int) -> alt.Chart:
    chart = chart.properties(width=chart_width, height=chart_height)
    return chart

def create_dot_plot(df, col_for_dots: str, 
                    x_axis_col:str, y_axis_col:str,
                   tooltip_cols:list, chart_title:str):
  
    chart = (alt.Chart(df).mark_circle(opacity=1, size = 200).transform_window(
    id='rank()',
    groupby=[col_for_dots]).encode(
    alt.X(f'{x_axis_col}:O', sort='ascending',
          scale=alt.Scale(domain=[0,10,20,30,40,50,60,70,80,90,100]), axis=alt.Axis(ticks=False, grid=True)),
    alt.Y(f'{y_axis_col}:N'), 
    color=alt.Color(f"{col_for_dots}:N", scale=alt.Scale(range=cp.CALITP_CATEGORY_BRIGHT_COLORS), legend=None),
    tooltip = tooltip_cols)
             .properties(title = chart_title))
    
    return chart

def create_text_table(df, chart_title:str):
   
    # Create a column for 0 to for the x axis
    df["Zero"] = 0
    
    chart = (
        (alt.Chart(df)
            .mark_circle()
            .encode(x=alt.X("Zero:Q", axis=None), 
            y=alt.Y("Full Information", axis=None))
            .properties(title=chart_title)))
    
    chart = (chart.mark_text(
        align="center",
        baseline="middle",
        dx=5)
        .encode(text="Full Information:N")
           )
    return chart

def bar_chart(df, x_axis_col:str, y_axis_col:str,
tooltip_cols:list, chart_title:str):
   
    chart = (
        alt.Chart(df)
        .mark_bar()
        .encode(
            x=alt.X(
                f"{x_axis_col}:Q",
                sort=alt.SortField(x_axis_col, order="descending"),
            ),
            y=alt.Y(
                f"{y_axis_col}:N", sort=alt.SortField(x_axis_col, order="descending")
            ),
            color=alt.Color(
                f"{y_axis_col}:N", scale=alt.Scale(range=cp.CALITP_CATEGORY_BRIGHT_COLORS), legend=None
            ),
            tooltip=tooltip_cols, 
        )
        .properties(title=chart_title)
        
    )
    
    return chart

def bar_chart_wo_dropdown(df, x_axis_col:str, y_axis_col:str,
tooltip_cols:list, chart_title:str):
    chart = (
        alt.Chart(df)
        .mark_bar()
        .encode(
            x=alt.X(
                f"{x_axis_col}:Q",
                sort=alt.SortField(x_axis_col, order="descending"),
            ),
            y=alt.Y(
                f"{y_axis_col}:N", sort=alt.SortField(x_axis_col, order="descending")
            ),
            color=alt.Color(
                f"{y_axis_col}:N", scale=alt.Scale(range=cp.CALITP_CATEGORY_BRIGHT_COLORS), legend=None
            ),
            tooltip=tooltip_cols, 
        )
        .properties(title=chart_title)
    )
    
    return chart

"""
Operator Visuals
& Analysis
"""
def route_summary_stats(df, column1:str, column2: str):
    """
    Dataframe that displays
    max, mean, median, and min of 
    route lengths for each operator.
    """
    df["Full Information"] = df[column1] + '-' + df[column2].astype(str) + "%"
    
    chart = create_text_table(df,  "Route Length Summary Stats")
    return chart

def valid_stats_leniency(df):
    """
    Return a df with the % of trips cut 
    when applying the most strigent
    and the  most leninent thresholds.
    """
    # Grab only max and min into the df 
    df = df.groupby(["Name"]).agg({"Percentage Usable Trips": ["max", "min"]}).reset_index()
    df.columns = df.columns.droplevel()
    df = df.rename(columns={"": "name",})
    
    df = pre_clean(df)
    
    # Concat all the stats into one column
    df['Full Information'] = 'Most Lenient: ' + df.Max.astype(str) + "%"  + ' Most Stringent: ' + df.Min.astype(str) + "%" 
    
    return df

def create_operator_visuals(height:int, width:int):
    """
    Create all 4 charts
    for the operators
    """
    route_length, time_segments, valid_stats = load_dataframes(TIME_CUTOFFS, SEGMENT_CUTOFFS)
    
    dropdown_list = route_length["Name"].sort_values().unique().tolist()
    dropdown = alt.binding_select(options=[None] + dropdown_list, labels = ['All'] + dropdown_list, name = "Operator")    
    selection = alt.selection_single(fields=["Name"], bind=dropdown)
    
    # Create charts
    route_length_chart = create_dot_plot(route_length, 
                                   'Variable', 
                                   'Rounded Route Length Percentage', 
                                   'Name', 
                                   ['Name', 'Gtfs Dataset Key', 'Variable', 'Route Length Percentage'],
                                "Length of Shape ID versus Longest Shape ID")
    route_text_chart = route_summary_stats(route_length, "Variable", "Route Length Percentage")
    valid_stats_chart = bar_chart(valid_stats, "Percentage Usable Trips",
                                  "Cutoff", 
                                  ['Gtfs Dataset Key', 'Name', 'Total Trips', 'N Trips','Cutoff', 'Percentage Usable Trips'],
                                  "Percentage of Usable Trips")
    
    leniency_text_chart = create_text_table(valid_stats_leniency(valid_stats), "Percentage of Trips Kept")
    
    
    route_length_chart= chart_size(route_length_chart, height, width).add_selection(selection).transform_filter(selection)
    route_text_chart= chart_size(route_text_chart, height, width).add_selection(selection).transform_filter(selection)
    valid_stats_chart= chart_size(valid_stats_chart, height, width).add_selection(selection).transform_filter(selection)
    leniency_text_chart= chart_size(leniency_text_chart, height, width).add_selection(selection).transform_filter(selection)
    
    return route_length_chart & route_text_chart &  valid_stats_chart & leniency_text_chart

def operator_brush(df):
    brush = alt.selection(type='interval')
    
    # Create chart
    chart = (alt.Chart(df).mark_circle(opacity=1, size = 150).transform_window(
    id='rank()',
    groupby=['Variable']).encode(
    alt.X('Rounded Route Length Percentage:O', sort='ascending',
          scale=alt.Scale(domain=[0,10,20,30,40,50,60,70,80,90,100]), axis=alt.Axis(ticks=False, grid=True)),
          alt.Y('Name:N'), 
    color=alt.Color("Variable:N", scale=alt.Scale(range=cp.CALITP_CATEGORY_BRIGHT_COLORS), legend=None),
    tooltip = ['Name', 'Gtfs Dataset Key', 'Variable', 'Route Length Percentage'])
     .properties(title = "Length of Shape ID versus Longest Shape ID").add_selection(brush))
   
    chart = chart_size(chart,500, 1000)
    
    # Create text table that corresponds with chart
    ranked_text = alt.Chart(df).mark_text().encode(
        y=alt.Y('row_number:O',axis=None)
    ).transform_window(
        row_number='row_number()'
    ).transform_filter(
        brush
    ).transform_window(
        rank='rank(row_number)'
    ).transform_filter(
        alt.datum.rank < 5
    )
    
    operator = ranked_text.encode(text='Name:N').properties(title='Operator')
    variable = ranked_text.encode(text='Variable:N').properties(title='Summary Statistics')
    route_length_percentage = ranked_text.encode(text='Route Length Percentage:Q').properties(title='Route Length Percentage')
    
    # Combine data tables
    text = alt.hconcat(operator, variable, route_length_percentage) 
    
    return alt.vconcat(chart, text)
"""
Statewide Visuals 
& Analysis
"""
def routes_left_thresholds():
    """
    Find number of routes that are cut
    and are left after applying thresholds
    """
    trips_routes_shape = merge_trips_routes_longest_shape()
    trip_stats = catalog.trip_stats.read()
    
    m1 = trip_stats.merge(
        trips_routes_shape.drop(columns=["route_length_percentage"]),
        how="inner",
        on=["gtfs_dataset_key", "route_dir_identifier"],
    )
    
    m1 = m1.assign(
        pct_vp_segments=m1.num_segments_with_vp.divide(m1.total_segments),
        trip_time=((m1.trip_end - m1.trip_start) / np.timedelta64(1, "s")) / 60,
    )
    
    total_unique_routes = m1.route_id.nunique() 
    
    routes = pd.DataFrame()
    for t in TIME_CUTOFFS:
        for s in SEGMENT_CUTOFFS:
            valid = (
                m1[(m1.trip_time >= t) & (m1.pct_vp_segments >= s)][["route_id"]]
                    .nunique()
                    .reset_index()
                    .rename(columns={0: "Total Routes in Category"})
                )

            valid = valid.assign(route_cutoff=f"{t} min, {s}% segments")

            routes = pd.concat([routes, valid], axis=0)
            
    routes = routes.assign(
            total_routes=total_unique_routes,
            percentage_of_routes_left=(routes["Total Routes in Category"].divide(
                total_unique_routes))*100,
            missing_routes = total_unique_routes - routes["Total Routes in Category"],)
    
    return routes

def statewide_cutoffs(df):
    """
    Find number of trips and % of trips
    retained after applying cutoffs for 
    the entire state.
    
    df: input results from `valid_stats`
    """
    # Find total trips across all operators.
    total_trips_state = df.groupby('Name')['Total Trips'].max().sum()
    
    df2 = df.groupby(['Cutoff']).agg({'N Trips':'sum'}).reset_index()
    
    df2['Percentage of Usable Trips'] = df2['N Trips']/total_trips_state * 100
    
    return df2

def create_statewide_visuals(height:int, width:int):
    
    route_length, time_segments, valid_stats = load_dataframes(TIME_CUTOFFS, SEGMENT_CUTOFFS)
    
    # Find # and percentage of trips left 
    # after applying cutoffs for the entire state
    statewide = statewide_cutoffs(valid_stats)
    
    # Create the chart
    statewide_chart = bar_chart_wo_dropdown(statewide, "Percentage of Usable Trips",
       "Cutoff", ["Cutoff", "Percentage of Usable Trips","N Trips"], 
        "Percentage of Usable Trips Across All Operators")
    
    # Find routes that are left after applying thresholds
    routes_left = routes_left_thresholds()
    
    # Clean up columns
    routes_left = pre_clean(routes_left)
    
    routes_chart = bar_chart_wo_dropdown(routes_left, "Percentage Of Routes Left",
       "Route Cutoff", ["Route Cutoff","Percentage Of Routes Left","Missing Routes"], 
        "Percentage of Routes Left after Applying Thresholds")
    
    statewide_chart= chart_size(statewide_chart, height, width)
    routes_chart= chart_size(routes_chart, height, width)
    
    return statewide_chart & routes_chart

def find_cut_routes(trip_time:int, segments_pct: float):
    """
    Find which routes are missing 
    after applying thresholds
    """
    trips_routes_shape = merge_trips_routes_longest_shape()
    trip_stats = catalog.trip_stats.read()
    
    m1 = trip_stats.merge(
        trips_routes_shape.drop(columns=["route_length_percentage"]),
        how="inner",
        on=["gtfs_dataset_key", "route_dir_identifier"],
    )
    
    m1 = m1.assign(
        pct_vp_segments=m1.num_segments_with_vp.divide(m1.total_segments),
        trip_time=((m1.trip_end - m1.trip_start) / np.timedelta64(1, "s")) / 60,
    )
    
    # Find routes that are retained
    kept_routes = m1[(m1["trip_time"] >= trip_time ) & (m1["pct_vp_segments"] >= segments_pct)][['name','route_id']].drop_duplicates()
    
    # Cast routes that are retained to a set
    routes_left_after_threshold = set(kept_routes.route_id.tolist())
    
    # Cast all routes into a set
    all_routes = set(m1.route_id.unique().tolist())
    
    # Find routes that are cut out after applying thresholds
    missing_routes_list = list(all_routes - routes_left_after_threshold)
    missing_routes_df = (m1[m1["route_id"]
                            .isin(missing_routes_list)][['name','gtfs_dataset_key','route_id',]]
                           .drop_duplicates()
                         .reset_index(drop = True)
                         .sort_values(by=['name','route_id'])
                        )
    
    missing_routes_df = pre_clean(missing_routes_df)
    
    missing_routes_df = (missing_routes_df
                         .groupby(['Gtfs Dataset Key','Name', 'Route Id'])
                         .agg({'Route Id':'nunique'})
                         .drop(columns=['Route Id'])
                        )
    return missing_routes_df