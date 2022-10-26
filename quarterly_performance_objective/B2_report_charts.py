import altair as alt
import pandas as pd

from typing import List, Literal

from shared_utils import styleguide
from shared_utils import calitp_color_palette as cp


def base_bar(df: pd.DataFrame) -> alt.Chart:
    chart = (alt.Chart(df)
             .mark_bar()
             .encode(
                 x=alt.X("District:N", title="District")
             )
            )
    return chart


def make_bar(df: pd.DataFrame, y_col: str) -> alt.Chart:
    """
    Make bar chart that's total service hours or 
    average service hours by district.
    """
    y_title = f"{y_col.replace('_', ' ').title()}"
    
    if y_col == "service_hours":
        value_format = ",.0f"
        y_buffer = 1_400
    elif y_col == "avg_delay_hours": 
        value_format = ",.1f"
        y_buffer = 1
    else:
        value_format = ",.1f"
        y_buffer = 5
    
    Y_MAX = df[y_col].max() + y_buffer
    
    bar = base_bar(df)
    
    bar = (bar.encode(
        y=alt.Y(f"{y_col}:Q", title=f"{y_title}", 
                scale=alt.Scale(domain=[0, Y_MAX]),
                axis=None
               ),
        color=alt.Color("District:N", 
                        scale=alt.Scale(
                            range=cp.CALITP_CATEGORY_BRIGHT_COLORS
                        ), legend=None
                )
             )
            )
    #https://stackoverflow.com/questions/54015250/altair-setting-constant-label-color-for-bar-chart
    text = (bar
            .mark_text(align="center", baseline="bottom",
                       color="black", dy=-5  
                      )
            .encode(text=alt.Text(y_col, format=value_format), 
                    # Set color here, because encoding for mark_text gets 
                    # superseded by alt.Color
                   color=alt.value("black"), 
                   tooltip=["District:N", 
                            alt.Tooltip(f"{y_col}:Q", format=",",
                                        title=f"{y_col.replace('_', ' '.title())}"
                                       )] 
                    
        )
    )
      
    chart = ((bar + text)
             .properties(title = {
                 "text": f"{y_title} by District",
                 "subtitle": "Routes on SHN"
             }).interactive()
            )
    
    return chart


def configure_hconcat_charts(
    my_chart_list: List[alt.Chart],
    x_scale: Literal["independent", "shared"] = "independent",
    y_scale: Literal["independent", "shared"] = "independent",
    chart_title: str = "Title"
) -> alt.Chart:
    """
    Horizontally concatenate altair charts, 
    and also add top-level configurations after hconcat is done
    """
    combined = (alt.hconcat(*my_chart_list)
                .resolve_scale(x = x_scale, y = y_scale)
               )
        

    combined = (styleguide.apply_chart_config(combined)
                .properties(title = chart_title)
                .configure_axis(grid=False)
                .configure_view(strokeWidth=0)
    )
    
    return combined