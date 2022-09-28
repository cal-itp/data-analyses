# Geography
import geopandas as gpd

# Charts
from shared_utils import geography_utils
from shared_utils import calitp_color_palette as cp
from shared_utils import altair_utils
import altair as alt

# Format
from babel.numbers import format_currency
from IPython.display import HTML, Image, Markdown, display, display_html

GCS_FILE_PATH = "gs://calitp-analytics-data/data-analyses/project_prioritization/"
"""
Chart Functions
"""
# Preset to make chart 25% smaller than shared_utils
# Since the webpage is kind of small
chart_width = 300
chart_height = 188

def preset_chart_config(chart: alt.Chart) -> alt.Chart:
    chart = chart.properties(
        width=chart_width,
        height=chart_height
    )
    return chart

# Labels for charts 
def labeling(word):
    # Add specific use cases where it's not just first letter capitalized
    if (word == "mpo") or (word == "rtpa"):
        word = word.upper()
    else:
        word = word.replace('unique_', "Number of Unique ").title()
        word = word.replace('_', ' ').title()
    return word

"""
Bar chart where the tooltip's value is another column that isn't x_col or y_col.
Convenient for graphs regarding monetary values because the tooltip will show the formatted
version.
"""
def basic_bar_chart_custom_tooltip(df, x_col, y_col, tooltip_col, colorcol, chart_title=''):
    if chart_title == "":
        chart_title = (f"{labeling(x_col)} by {labeling(y_col)}")
    chart = (alt.Chart(df)
             .mark_bar()
             .encode(
                 x=alt.X(x_col, title=labeling(x_col), axis=alt.Axis(labels=False)),
                 y=alt.Y(y_col, title=labeling(y_col),sort=('-x')),
                 color = alt.Color(colorcol, 
                                  scale=alt.Scale(
                                      range=cp.CALITP_CATEGORY_BRIGHT_COLORS),
                                      legend=alt.Legend(title=(labeling(colorcol)))
                                  ),
                tooltip = [tooltip_col])
             .properties( 
                       title=chart_title)
    )
    chart = preset_chart_config(chart)

    return chart

def dual_bar_chart(df, control_field:str, chart1_nominal:str,
                  chart1_quant: str, chart2_nominal:str, 
                  chart2_quant:str, chart1_tooltip_cols: list,
                  chart2_tooltip_cols: list):
    """An interactive dual bar chart
    https://stackoverflow.com/questions/53404826/how-to-link-two-bar-charts-in-altair
    
    Args:
        df: the dataframe
        control_field (str): the field of the 1st chart that controls your 2nd chart.
        chart1_nominal (str): nominal column for 1st bar chart. should be in format 'column_name:N'
        chart1_quant (str): quantitive column for 1st bar chart. should be in format 'column_name:Q'
        chart2_nominal (str): nominal column for 2nd bar chart. should be in format 'column_name:N'
        chart2_quant (str): quantitive column for 2nd bar chart. should be in format 'column_name:Q'
        chart1_tooltip_cols (list): list of columns to place in tooltip in chart 1
        chart2_tooltip_cols (list): list of columns to place in tooltip chart 2
    Returns:
        Returns two horizontally concated bar charts: the first bar chart controls the second bar chart.
    """
    # Column that controls the bar charts
    category_selector = alt.selection_multi(fields=[control_field])
    
    # Build first chart
    chart1 = (alt.Chart(df).mark_bar().encode(
        x=alt.X(chart1_quant, axis=alt.Axis(labels=False)),
        y=alt.Y(chart1_nominal), 
        color = alt.Color(chart1_quant, scale=alt.Scale(
        range=cp.CALITP_CATEGORY_BRIGHT_COLORS), legend = None),
        tooltip = chart1_tooltip_cols)
        .add_selection(category_selector))
    
    # Build second chart
    chart2 = (alt.Chart(df).mark_bar().encode(
        x=alt.X(chart2_quant, axis=alt.Axis(labels=False)),
        y=alt.Y(chart2_nominal, ), 
        color = alt.Color(chart2_quant, scale=alt.Scale(
        range=cp.CALITP_CATEGORY_BRIGHT_COLORS), legend = None),
        tooltip = chart2_tooltip_cols)
        .transform_filter(category_selector))
    chart1 = preset_chart_config(chart1)
    chart2 = preset_chart_config(chart2)
    return(chart1 | chart2)

def basic_pie_chart(df, quant_col:str, nominal_col:str, label_col:str,
                   chart_title:str):
    """
    quant_col should be "Column Name:Q"
    nominal_col should be "Column Name:N"
    label_col should be "Column Name:N"
    """
    # Base Chart
    base = (alt.Chart(df)
            .encode(theta=alt.Theta(quant_col, stack=True), 
            color=alt.Color(nominal_col, 
            scale = alt.Scale(range = cp.CALITP_CATEGORY_BRIGHT_COLORS),
            legend = alt.Legend(title=labeling(label_col))),
            tooltip = [label_col,quant_col])
            .properties(title=chart_title)
           )
    # Set size of the pie 
    pie = base.mark_arc(outerRadius=80)
    
    # Add labels
    text = base.mark_text(radius=100, size=10).encode(text=label_col)
    
    # Combine the chart and labels
    chart =  preset_chart_config(pie + text)
    
    return chart

"""
Other Functions
"""
# Grab value counts and turn it into a dataframe
def value_counts_df(df, col_of_interest):
    df = (
    df[col_of_interest]
    .value_counts()
    .to_frame()
    .reset_index()
    )
    return df 

# Strip snakecase when dataframe is finalized
def clean_up_columns(df):
    df.columns = df.columns.str.replace("_", " ").str.title().str.strip()
    return df

"""
Style the dataframe by removing the index and gray banding,
dropping certain columns and centering text. Adds scrollbar
"""
def styled_df(df):
    display(
    HTML(
        "<div style='height: 500px; overflow: auto; width: fit-content'>"
        + (
            (df)
            .style.set_properties(**{"background-color": "white"})
            .set_table_styles([dict(selector="th", props=[("text-align", "center")])])
            .set_properties(**{"text-align": "center"})
            .hide(axis="index")
            .render()
        )
        + "</div>"
    ))

"""
Create summary table: returns total projects and total cost
by the column of your choice. 
"""
def summarize_by_project_names(df, col_wanted: str):
    """
    df: original dataframe to summarize
    col_wanted: to column to groupby
    """
    df = (
        df.groupby([col_wanted])
        .agg({"project_name": "count", "total_project_cost__$1,000_": "sum"})
        .reset_index()
        .sort_values("project_name", ascending=False)
        .rename(columns={"project_name": "Total Projects"})
    )

    df = df.reset_index(drop=True)

    # Create a formatted monetary col
    df["Total Project ($1000) Formatted"] = df["total_project_cost__$1,000_"].apply(
        lambda x: format_currency(x, currency="USD", locale="en_US")
    )

    # Clean up column names, remove snakecase
    df = clean_up_columns(df)

    return df

"""
Merge a dataframe with Caltrans District Map 
to return a gdf
"""
Caltrans_shape = "https://gis.data.ca.gov/datasets/0144574f750f4ccc88749004aca6eb0c_0.geojson?outSR=%7B%22latestWkid%22%3A3857%2C%22wkid%22%3A102100%7D"

def create_caltrans_map(df):
    
    # Load in Caltrans shape
    ct_geojson = gpd.read_file(f"{Caltrans_shape}").to_crs(epsg=4326)
    
    # Keep only the columns we want
    ct_geojson = ct_geojson[["DISTRICT", "Shape_Length", "Shape_Area", "geometry"]]
    
    # Inner merge 
    districts_gdf = ct_geojson.merge(
    df, how="inner", left_on="DISTRICT", right_on="District")
    
    return districts_gdf