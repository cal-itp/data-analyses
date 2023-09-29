"""
Chart utility functions to use in reporting notebook.

Report: tier1-facilities-hqta.ipynb
"""
import altair as alt
import pandas as pd

from calitp_data_analysis import calitp_color_palette as cp
from calitp_data_analysis import styleguide

def make_donut_chart(df: pd.DataFrame, y_col: str, color_col: str) -> alt.Chart:

    chart = (alt.Chart(df)
             .mark_arc(innerRadius=75)
             .encode(
                theta=alt.Theta(f"{y_col}:Q", stack=True),
                color=alt.Color(f"{color_col}:N", 
                                scale=alt.Scale(
                                    range=cp.CALITP_CATEGORY_BRIGHT_COLORS)
                               ),
                tooltip = [
                    color_col,
                    alt.Tooltip(f'{y_col}:Q', title='number'), 
                    alt.Tooltip("pct:Q", title="pct", format=".1%")
                ]
        )
    )
    
    text = (chart
            .mark_text(align="right", 
                       #baseline="bottom", 
                       fontSize=16,
                       radius = 170)
            .encode(
                text = f"{y_col}:Q",
                color=alt.value("black")
            )
    )
    # https://stackoverflow.com/questions/72083560/semi-circle-donut-chart-with-altair
    #pie + pie.mark_text(radius=170, fontSize=16).encode(text='category')

    return chart


def make_bar_chart(df: pd.DataFrame, x_col: str) -> alt.Chart:
    x_label = f"{x_col.replace('_name', '').title()}"
    
    y_col = "num_facilities"
    #Y_MAX = df[y_col].max()
    
    chart = (alt.Chart(df)
             .mark_bar(binSpacing=10)
             .encode(
                 x=alt.X(f"{x_col}:N", title = f"{x_label}"),
                 y=alt.Y(f"{y_col}:Q", title="# Facilities", 
                         #scale=alt.Scale(domain=[0, Y_MAX])
                        ),
                 color=alt.Color("category:N",
                                 scale=alt.Scale(range=cp.CALITP_CATEGORY_BRIGHT_COLORS)
                                ),
                 tooltip=[alt.Tooltip(x_col, title=x_label),
                          "category",
                          alt.Tooltip(f"{y_col}", title="# Facilities"),  
                 ]
             ).properties(title = f"Number of Facilities by {x_label}")
            )
    
    chart = styleguide.preset_chart_config(chart)
    
    return chart
