import altair as alt
import altair_saver
import matplotlib.pyplot as plt
import pandas as pd
import seaborn as sns

import utils
import shared_utils

from shared_utils import styleguide, geography_utils
from shared_utils import calitp_color_palette as cp

# Set charting style guide
alt.data_transformers.enable('default', max_rows=10_000)
alt.renderers.enable('altair_saver', fmts=['png'])

plt.style.use('../_shared_utils/shared_utils/calitp.mplstyle')


def grab_legend_thresholds(df: pd.DataFrame, 
                           plot_col: str,
                           arrivals_group: str) -> tuple[float]:
    cut1 = df[(df[arrivals_group]==1) & (df[plot_col] > 0)][plot_col].min()
    cut2 = df[df[arrivals_group]==2][plot_col].min()
    cut3 = df[df[arrivals_group]==3][plot_col].min()

    MIN_VALUE = df[plot_col].min()
    MAX_VALUE = df[plot_col].max()
    
    colormap_cutoff = [cut1, cut2, cut3]
    
    return colormap_cutoff, MIN_VALUE, MAX_VALUE

#----------------------------------------------------------------#
## Bar charts
#----------------------------------------------------------------#
LEGEND_LABELS = {
    1: "Low",
    2: "Med", 
    3: "High",
}

LEGEND_ORDER = list(LEGEND_LABELS.values())

CHART_LABELING = {
    "arrivals_per_1k_pj" : "Arrivals per 1k by Pop / Job Density",
    "equity_group": "CalEnviroScreen",
    "popjobdensity_group": "Pop / Job Density",
}

def labeling(word: str) -> str:
    if word in CHART_LABELING.keys():
        word = CHART_LABELING[word]
    else: 
        word = word.replace('_', ' ').title()
    return word


def chartnaming(word: str) -> str:
    word = word.replace('_pj', '').replace('_group', '')
    return word


def base_bar_chart(df: pd.DataFrame, x_col: str, 
                   y_col: str, Y_MAX: int) -> alt.Chart:
    bar = (alt.Chart(df)
           .mark_bar()
           .encode(
               x=alt.X(f"{x_col}:N", 
                       axis=alt.Axis(title=""), 
                       scale=alt.Scale(domain=LEGEND_ORDER)),
               y=alt.Y(f"{y_col}:Q", 
                       axis=alt.Axis(title=labeling(y_col)),
                       scale=alt.Scale(domain=[0, Y_MAX])
                      ),
           )
    )
           
    return bar
    

def bar_chart(df: pd.DataFrame, x_col: str, y_col: str, 
              color_col: str, Y_MAX: int, 
              chart_title: str, IMG_PATH = f"{utils.IMG_PATH}"):
    
    # Need to add a way to find the max across different datasets...
    # So charts are comparable in presentation
    
    bar = base_bar_chart(df, x_col, y_col, Y_MAX)
    
    bar = (bar.encode(
               color=alt.Color(f"{color_col}:N", 
                               title=labeling(color_col),
                               scale=alt.Scale(domain=LEGEND_ORDER,
                                               range=cp.CALITP_CATEGORY_BRIGHT_COLORS
                                              )
                              )
           )
    )
    
    bar = (styleguide.preset_chart_config(bar)
           .properties(title=chart_title, width = styleguide.chart_width*0.25)
          )
    
    bar.save(f"{IMG_PATH}{chartnaming(y_col)}_{chartnaming(x_col)}.png")


def grouped_bar_chart(df: pd.DataFrame, 
                      x_col: str, y_col: str, 
                      color_col: str, grouped_col: str, 
                      Y_MAX: int, chart_title: str, 
                      IMG_PATH = f"{utils.IMG_PATH}"):    
    bar = base_bar_chart(df, x_col, y_col, Y_MAX)
    bar = (bar.encode(
            column = alt.Column(f"{grouped_col}:N", 
                                title=labeling(grouped_col)
                               ),
            color=alt.Color(f"{color_col}:N", 
                            title=labeling(color_col),
                            scale=alt.Scale(domain=LEGEND_ORDER,
                                           range=cp.CALITP_CATEGORY_BRIGHT_COLORS)
                           )
        )
    )
    bar = (styleguide.preset_chart_config(bar)
           .properties(title=chart_title, width = styleguide.chart_width*0.25)
          )
    
    bar.save(f"{IMG_PATH}{chartnaming(y_col)}_{chartnaming(x_col)}_bar.png")

    
#----------------------------------------------------------------#
## Heatmaps
#----------------------------------------------------------------#
def make_heatmap(df: pd.DataFrame, chart_title: str, xtitle: str, ytitle: str):
    chart = (sns.heatmap(df, cmap="Blues")
             .set(title=chart_title, xlabel=xtitle, ylabel=ytitle,
                 )
    )
    
    return chart

    
#----------------------------------------------------------------#
## Scatterplots
#----------------------------------------------------------------#
def make_scatterplot(df: pd.DataFrame, 
                     x_col: str, y_col: str, 
                     group_col: str, tooltip_list: list, 
                     chart_title: str) -> alt.Chart:
    
    # Interactive chart should be centered near where 95th percentile data is
    # There are outliers...but let's not have it so zoomed out
    
    def colname_label(col: str) -> (str, str):
        colname = col.split(":")[0]
        coltitle = colname.replace('_', ' ').replace(' pj', '').replace('popjobs', 'pop/job')
        return colname, coltitle
    
    x_colname, x_title = colname_label(x_col)
    y_colname, y_title = colname_label(y_col)
    
    # Draw dotted horiz and vertical lines for where the 50th percentile is
    # Display only p to 95th percentile
    MAX_X = df[x_colname].quantile(q=0.95)
    MAX_Y = df[y_colname].quantile(q=0.95)
    MEDIAN_X = df[x_colname].quantile(q=0.5)
    MEDIAN_Y = df[y_colname].quantile(q=0.5)
    
    subset_df = (df[((df[x_colname] > 0) 
                     & (df[y_colname] > 0))]
                 .assign(
                     median_x = MEDIAN_X,
                     median_y = MEDIAN_Y
                 )
                )
    
    chart = (
        alt.Chart(subset_df)
        .mark_point(size=30, fillOpacity=0.8)
        .encode(
            x=alt.X(x_col, 
                    scale=alt.Scale(domain=(0, MAX_X)), 
                    axis=alt.Axis(title=f"{x_title}")
                   ),
            y=alt.Y(y_col, 
                    scale=alt.Scale(domain=(0, MAX_Y)),
                    axis=alt.Axis(title=f"{y_title}")
                   ),
            color=group_col,
            tooltip=tooltip_list
        )
    )
    
    ptile_base = (
        alt.Chart(subset_df)
        .mark_line(clip=True)
    )
    
    
    fifty_x = (ptile_base.encode(
            x=alt.X("median_x:Q"),
            y=alt.Y(y_col),
            color=alt.value("#343537")
        )
    )
    
    fifty_y = (ptile_base.encode(
            x=alt.X(x_col),
            y=alt.Y("median_y:Q"),
            color=alt.value("#343537")
        )
    )
    
    full_chart = chart + fifty_x + fifty_y
    full_chart = (styleguide.preset_chart_config(full_chart)
             .properties(title=f"{chart_title}")
             .interactive()
            )
    
   
    return full_chart
