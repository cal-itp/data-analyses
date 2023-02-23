import geopandas as gpd
import numpy as np
import pandas as pd
from calitp.sql import to_snakecase

import intake
catalog = intake.open_catalog("./catalog_threshold.yml")

from shared_utils import calitp_color_palette as cp
from shared_utils import rt_utils
from shared_utils import geography_utils, styleguide, utils
import altair as alt

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
        df.groupby(["name", "route_id", "shape_id"])
        .agg({"route_length_percentage": "max"})
        .reset_index()
    )
    
    # Get summary stats
    df = (
        df.groupby("name")
        .agg({"route_length_percentage": ["mean", "median", "min", "max"]})
        .reset_index()
    )
    
    # Drop index
    df.columns = df.columns.droplevel()
 
    df = df.rename(columns={"": "name",})
    
    # Melt to long df
    df = pd.melt(df, id_vars=["name"], value_vars=["mean", "median", "min", "max"])
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

"""
Charts
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

# Format the size of a chart
def chart_size(chart: alt.Chart, chart_width: int, chart_height: int) -> alt.Chart:
    chart = chart.properties(width=chart_width, height=chart_height)
    return chart

