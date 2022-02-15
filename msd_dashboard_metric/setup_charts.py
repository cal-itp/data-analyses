import altair as alt
import pandas as pd

from shared_utils import styleguide
from shared_utils import calitp_color_palette as cp

AXIS_DATE_FORMAT ="%-m/%-d/%y"


# Histogram for % has stop or trip accessibility info
def labeling(word):
    word = word.replace('pct_has_', '').replace('_', ' ')
    
    return word


def make_histogram(df, x_col):
    x_title = f"{labeling(x_col)} information"
             
    chart = (alt.Chart(df)
             .mark_bar()
             .encode( 
                 x=alt.X(f"{x_col}:Q", bin=True, title=f"% {x_title}",
                        axis=alt.Axis(format="%")),
                 y=alt.Y("count():Q", title="# Feeds"),
                 color=alt.value(cp.CALITP_CATEGORY_BRIGHT_COLORS[0]),
                 #Tooltip for aggregates: https://github.com/altair-viz/altair/issues/1065
                 #Tooltip for histogram: https://github.com/altair-viz/altair/issues/2006
                 tooltip=[alt.Tooltip(x_col, bin=True, title="bin"), 
                          alt.Tooltip("count()", title="count")]
             )
    )
    
    chart = (styleguide.preset_chart_config(chart)
             .properties(title=f"Feeds by % {x_title.title()}")
             .interactive()
            )
    
    display(chart)
    
    
def make_bar_chart(df, x_col, y_col, chart_title):
    if y_col == "value":
        y_title = "# feeds"
    elif y_col == "pct":
        y_title = "% feeds"
        
    chart = (alt.Chart(df)
             .mark_bar(size=35)
             .encode(
                 x=alt.X(x_col),
                 y=alt.Y(y_col, title=y_title),
                 color=alt.value(cp.CALITP_CATEGORY_BRIGHT_COLORS[0]),
                 tooltip=[x_col, y_col]
             ).properties(title=chart_title)
    )

    chart = (styleguide.preset_chart_config(chart)
             .properties(width = styleguide.chart_width*0.4)
             .interactive()
            )
    
    display(chart)
    
    

def base_line_chart(df):
    chart = (alt.Chart(df)
             .mark_line()
             .encode(
                 x=alt.X("date", axis=alt.Axis(format=AXIS_DATE_FORMAT))
             )
            )
    return chart