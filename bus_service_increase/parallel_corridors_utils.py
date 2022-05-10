"""
Functions backing competitive-parallel-routes.ipynb.

Create stripplots and stats used in narrative.
"""
import altair as alt
import intake
import pandas as pd

from IPython.display import display, Markdown, HTML

import setup_parallel_trips_with_stops
import utils
from shared_utils import calitp_color_palette as cp
from shared_utils import styleguide

alt.themes.register("calitp_theme", styleguide.calitp_theme)

catalog = intake.open_catalog("./*.yml")

SELECTED_DATE = '2022-1-6' #warehouse_queries.dates['thurs']

def operator_parallel_competitive_stats(itp_id, pct_trips_competitive_cutoff):
    '''
    DATA_PATH = f"{utils.GCS_FILE_PATH}2022_Jan/"

    # Read in intermediate parquet for trips on selected date
    trips = pd.read_parquet(f"{DATA_PATH}trips_joined_thurs.parquet")

    # Attach service hours
    # This df is trip_id-stop_id level
    trips_with_service_hrs = setup_parallel_trips_with_stops.grab_service_hours(
        trips, SELECTED_DATE)

    trips_with_service_hrs.to_parquet("./data/trips_with_service_hours.parquet")
    '''    
    df = pd.read_parquet("./data/trips_with_service_hours.parquet")
    df = df[df.calitp_itp_id==itp_id]
    
    parallel_df = setup_parallel_trips_with_stops.subset_to_parallel_routes(df)
    
    competitive_df = catalog.competitive_route_variability.read()
    competitive_df = competitive_df[
        (competitive_df.calitp_itp_id == itp_id) & 
        (competitive_df.pct_trips_competitive > pct_trips_competitive_cutoff)
    ]
    
    operator_dict = {
        "num_routes": df.route_id.nunique(),
        "parallel_routes": parallel_df.route_id.nunique(),
        "competitive_routes": competitive_df.route_id.nunique(),
    }
    
    return operator_dict

#------------------------------------------------------------#
# Stripplot
# #https://altair-viz.github.io/gallery/stripplot.html
#------------------------------------------------------------#
# Color to designate p25, p50, p75, fastest trip?
DARK_GRAY = "#323434"
#NAVY = cp.CALITP_CATEGORY_BOLD_COLORS[0]

def labeling(word):
    label_dict = {
        "bus_multiplier": "Ratio of Bus to Car Travel Time",
        "bus_difference": "Difference in Bus to Car Travel Time (min)"
    }
    
    if word in label_dict.keys():
        word = label_dict[word]
    else:
        word = word.replace('_', ' ').title()
    
    return word


def specific_point(y_col):
    chart = (
        alt.Chart()
        .mark_point(size=20, opacity=0.6, strokeWidth=1.3)
        .encode(
            y=alt.Y(f'{y_col}:Q'),
            color=alt.value(DARK_GRAY)
        )
    )
    
    return chart


diff_cutoffs = {
    "short": 20,
    "medium": 30,
    "long": 40,
}

def make_stripplot(df, y_col="bus_multiplier", Y_MIN=0, Y_MAX=5):
    # Instead of doing +25% travel time, just use set cut-offs because it's easier
    # to write caption for across operators
    df = df.assign(
        cutoff2 = diff_cutoffs[df.route_group.iloc[0]]
    )
    
    # We want to draw horizontal line on chart
    if y_col == "bus_multiplier":
        df = df.assign(cutoff=2)
        # if that operator falls well below cut-off, we want the horiz lines to be shown
        # take the max and add some buffer so horiz line can be seen
        Y_MAX = max(df.cutoff.iloc[0] + 0.5, Y_MAX)
        Y_MIN = min(-0.25, Y_MIN)
        
    elif y_col == "bus_difference":
        df = df.assign(cutoff=0)
        Y_MAX = max(df.cutoff2.iloc[0] + 5, Y_MAX)
        Y_MIN = min(-5, Y_MIN)
        
    # Use the same sorting done in the wrangling
    route_sort_order = list(df.sort_values(["calitp_itp_id", 
                                            "pct_trips_competitive", 
                                            "num_competitive",
                                            "p50"], 
                                       ascending=[True, False, False, True]
                                      )
                        .drop_duplicates(subset=["route_id"]).route_id)
        
    stripplot =  (
        alt.Chart()
          .mark_point(size=12, opacity=0.65, strokeWidth=1.1)
          .encode(
            x=alt.X(
                'jitter:Q',
                title=None,
                axis=alt.Axis(values=[0], ticks=True, grid=False, labels=False),
                scale=alt.Scale(),
                #stack='zero',
            ),
            y=alt.Y(f'{y_col}:Q', title=labeling(y_col), 
                    scale=alt.Scale(domain=[Y_MIN, Y_MAX])
                   ),
            color=alt.Color('time_of_day:N', title="Time of Day", 
                            sort=["AM Peak", "Midday", "PM Peak", "Owl Service"],
                            scale=alt.Scale(
                                # Grab colors where we can distinguish between groups
                                range=(cp.CALITP_CATEGORY_BOLD_COLORS
                                       #[:2] + 
                                       #cp.CALITP_CATEGORY_BOLD_COLORS[4:] 
                                      )
                            )
                           ),
            tooltip=alt.Tooltip(["route_id", "trip_id", 
                                 "service_hours", "car_duration_hours",
                                 "bus_multiplier", "bus_difference", 
                                 "num_trips", "num_competitive",
                                 "pct_trips_competitive",
                                 "p25", "p50", "p75"
                                ])
          )
        ).transform_calculate(
            # Generate Gaussian jitter with a Box-Muller transform
            jitter='sqrt(-2*log(random()))*cos(2*PI*random())'
    )
    
    p50 = (specific_point(y_col)
           .transform_filter(alt.datum.p50_trip==1)
          )

    horiz_line = (
        alt.Chart()
        .mark_rule()
        .encode(
            y=alt.Y("cutoff:Q"),
            color=alt.value("black")
        )
    )
    
    horiz_line2 = (
        alt.Chart()
        .mark_rule(strokeDash=[3,3])
        .encode(
            y=alt.Y("cutoff2:Q"),
            color=alt.value(DARK_GRAY)
        )
    )
    
    # Add labels
    # https://github.com/altair-viz/altair/issues/920
    text = (stripplot
            .mark_text(align='center', baseline='middle')
            .encode(
                x=alt.value(30),
                y=alt.value(15),
                text=alt.Text('pct_trips_competitive:Q', format='.0%'), 
                color=alt.value("black"))
           ).transform_filter(alt.datum.fastest_trip==1)
        
    # Must define data with top-level configuration to be able to facet
    if y_col == "bus_difference":
        horiz_charts = (horiz_line + horiz_line2)
        other_charts = p50 + text
    else:
        horiz_charts = horiz_line
        other_charts = p50 + text
    
    chart = (
        (horiz_charts + 
         stripplot.properties(width=50) + 
         other_charts)
        .facet(
            column = alt.Column("route_id:N", title="Route ID",
                                sort = route_sort_order), 
            data=df,
        ).interactive()
        .configure_facet(spacing=0)
        .configure_view(stroke=None)
        .resolve_scale(y='shared')
        .properties(title=labeling(y_col))
    )
        
    return chart



