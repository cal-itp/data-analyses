import altair as alt
import pandas as pd

from typing import List, Literal

from calitp_data_analysis import styleguide
from calitp_data_analysis import calitp_color_palette as cp

def base_bar(df: pd.DataFrame, x_col: str) -> alt.Chart:
    chart = (alt.Chart(df)
             .mark_bar()
             .encode(
                 x=alt.X(f"{x_col}:N", title=f"{x_col.title()}")
             )
            )
    return chart


def make_district_bar(df: pd.DataFrame, y_col: str) -> alt.Chart:
    """
    Make bar chart that's total service hours or 
    average service hours by district.
    """
    y_title = f"{y_col.replace('_', ' ').title()}"
    
    if y_col == "service_hours":
        value_format = ",.0f"
    elif y_col == "avg_delay_hours": 
        value_format = ",.1f"
    else:
        value_format = ",.1f"
    
    Y_MAX = df[y_col].max() * 1.1
    
    bar = base_bar(df, x_col="district")
    
    bar = (bar.encode(
        y=alt.Y(f"{y_col}:Q", title=f"{y_title}", 
                scale=alt.Scale(domain=[0, Y_MAX]),
                axis=None
               ),
        color=alt.Color("district:N", 
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
                   tooltip=["district:N", 
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