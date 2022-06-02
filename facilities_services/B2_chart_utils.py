import altair as alt

from shared_utils import calitp_color_palette as cp
from shared_utils import styleguide

def make_donut_chart(df, y_col, color_col):

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


def make_bar_chart(df, x_col):
    x_label = f"{x_col.replace('_name', '').title()}"
    
    chart = (alt.Chart(df)
             .mark_bar(binSpacing=10)
             .encode(
                 x=alt.X(f"{x_col}:N", title = f"{x_label}"),
                 y=alt.Y(f"num_facilities:Q", title="# Facilities", 
                         scale=alt.Scale(domain=[0, 14])
                        ),
                 color=alt.Color("category:N",
                                 scale=alt.Scale(range=cp.CALITP_CATEGORY_BRIGHT_COLORS)
                                ),
                 tooltip=[alt.Tooltip(x_col, title=x_label),
                          "category",
                          alt.Tooltip("num_facilities", title="# Facilities"),  
                 ]
             ).properties(title = f"Number of Facilities by {x_label}")
            )
    
    chart = styleguide.preset_chart_config(chart)
    
    return chart
