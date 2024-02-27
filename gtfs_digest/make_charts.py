"""
Chart functions.
"""
import altair as alt
import pandas as pd

from calitp_data_analysis import calitp_color_palette as cp

def base_route_chart(df: pd.DataFrame, y_col: str) -> alt.Chart:
    
    df = df.reset_index(drop=True)
    
    selected_colors = cp.CALITP_CATEGORY_BRIGHT_COLORS[3:]
    
    snakecase_ycol = y_col.replace('_', ' ')
    
    #https://stackoverflow.com/questions/26454649/python-round-up-to-the-nearest-ten
    max_y = round(df[y_col].max(), -1)
    
    chart = (
        alt.Chart(df)
        .mark_line()
        .encode(
             x = alt.X("yearmonthdate(service_date):O", title = "Date",
                       axis = alt.Axis(format = '%b %Y')
                      ),
             y = alt.Y(f"{y_col}:Q", title = snakecase_ycol,
                       scale = alt.Scale(domain=[0, max(max_y, 45)])
                      ),
             color = alt.Color(
                 "time_period:N", title = "",
                 scale = alt.Scale(range = selected_colors)
             ),
             tooltip = ["route_combined_name", "route_id", "direction_id", 
                        "time_period", y_col]
         ).facet(
             column = alt.Column("direction_id:N", title="Direction"),
             row = alt.Row("route_combined_name:O", title="Route")
         ).interactive()
    )
    
    return chart


def make_stripplot(
    df: pd.DataFrame, 
    one_route: str
) -> alt.Chart:
    
    chart = alt.Chart(
        df[(df.route_combined_name == one_route) & 
           (df.time_period != "all_day")], 
        title=f"{one_route}"
    ).mark_circle(
        size=20, opacity=0.65, strokeWidth=1.1
    ).encode(
        x="stop_pair:N",
        y="p50_mph:Q",
        yOffset="jitter:Q",
        color=alt.Color(
            "time_period:N",
            sort = ["peak", "offpeak"],
            title = "Time Period",
            scale=alt.Scale(
                # Grab colors where we can distinguish between groups
                range=[
                    cp.CALITP_CATEGORY_BRIGHT_COLORS[2], # yellow
                    cp.CALITP_CATEGORY_BRIGHT_COLORS[3], # green
                ]
            )            
        ),
        tooltip = ["route_id", "route_combined_name", 
                   "service_date", "time_period", 
                   "stop_pair", 
                   "p20_mph", "p50_mph", "p80_mph",
                  ]
    ).transform_calculate(
        # Generate Gaussian jitter with a Box-Muller transform
        jitter="random()"#"sqrt(-2*log(random()))*cos(2*PI*random())"
    ).properties(
        width = 500, height = 250
    ).interactive()

    return chart


def dual_chart(
    df, 
    control_field: str,
    y_col: str
):
    """
    https://stackoverflow.com/questions/58919888/multiple-selections-in-altair
    """
    route_dropdown = alt.binding_select(
        options=sorted(df[control_field].unique().tolist()), 
        name='Routes '
    )
        
    # Column that controls the bar charts
    route_selector = alt.selection_point(
        fields=[control_field], 
        bind=route_dropdown
    )
                
    chart = base_chart(
        df, y_col
    ).add_params(route_selector).transform_filter(route_selector)
   
    return chart