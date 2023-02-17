from shared_utils import calitp_color_palette as cp
from shared_utils import rt_utils
from shared_utils import geography_utils, styleguide, utils
import altair as alt

# Reverse snake_case
def clean_up_columns(df):
    df.columns = df.columns.str.replace("_", " ").str.strip().str.title()
    return df

# Format the size of a chart
def chart_size(chart: alt.Chart, chart_width: int, chart_height: int) -> alt.Chart:
    chart = chart.properties(width=chart_width, height=chart_height)
    return chart
