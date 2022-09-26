import altair as alt
from shared_utils import geography_utils
from shared_utils import calitp_color_palette as cp
from shared_utils import altair_utils
from babel.numbers import format_currency

'''
Chart Functions
'''
# Make chart 75% smaller than ours
# Since the webpage is kind of small
chart_width = 300
chart_height = 188

def preset_chart_config(chart: alt.Chart) -> alt.Chart:
    chart = chart.properties(
        width=chart_width,
        height=chart_height
    )

    return chart

#Labels for charts 
def labeling(word):
    # Add specific use cases where it's not just first letter capitalized
    if (word == "mpo") or (word == "rtpa"):
        word = word.upper()
    else:
        word = word.replace('unique_', "Number of Unique ").title()
        word = word.replace('_', ' ').title()
    return word

# Bar chart where the tooltip values is another column that isn't x_col or y_col 
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
                  chart2_quant:str, ):
    """An interactive dual bar chart
    https://stackoverflow.com/questions/53404826/how-to-link-two-bar-charts-in-altair
    
    Args:
        df: the dataframe
        control_field (str): the field of the 1st chart that controls your 2nd chart.
        chart1_nominal (str): nominal column for 1st bar chart. should be in format 'column_name:N'
        chart1_quant (str): quantitive column for 1st bar chart. should be in format 'column_name:Q'
        chart2_nominal (str): nominal column for 2nd bar chart. should be in format 'column_name:N'
        chart2_quant (str): quantitive column for 2nd bar chart. should be in format 'column_name:Q'
    Returns:
        Returns two horizontally concated bar charts: the first bar chart controls the second bar chart.
    """
    # Column that controls the bar charts
    category_selector = alt.selection_multi(fields=[control_field])
    
    # Build Chart one
    chart1 = (alt.Chart(df).mark_bar().encode(
        x=alt.X(chart1_quant, axis=alt.Axis(labels=False)),
        y=alt.Y(chart1_nominal), 
        color = alt.Color(chart1_quant, scale=alt.Scale(
        range=cp.CALITP_CATEGORY_BRIGHT_COLORS), legend = None),
        tooltip = [chart1_nominal, chart1_quant,])
        .add_selection(category_selector))

    chart2 = (alt.Chart(df).mark_bar().encode(
        x=alt.X(chart2_quant, axis=alt.Axis(labels=False)),
        y=alt.Y(chart2_nominal, ), 
        color = alt.Color(chart2_quant, scale=alt.Scale(
        range=cp.CALITP_CATEGORY_BRIGHT_COLORS), legend = None),
        tooltip = [chart2_nominal,chart2_quant,])
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
    # Bar Chart
    base = (alt.Chart(df)
            .encode(theta=alt.Theta(quant_col, stack=True), 
            color=alt.Color(nominal_col, 
            scale = alt.Scale(range = cp.CALITP_CATEGORY_BRIGHT_COLORS),
            legend = alt.Legend(title=labeling(label_col))),
            tooltip = [label_col,quant_col])
            .properties(title=chart_title)
           )
    # Create pie 
    pie = base.mark_arc(outerRadius=80)
    
    # Add text
    text = base.mark_text(radius=100, size=10).encode(text=label_col)
    
    chart =  preset_chart_config(pie + text)
    
    return chart

'''
Other Functions
'''
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
Style the dataframe by removing the index and gray banding, dropping certain columns
and centering text.
"""
def style_dataframe(df, hide_cols: list):
    df_styled = (
        df.drop(columns=hide_cols)
        .style.hide(axis="index")
        .set_properties(**{"background-color": "white"})
        .set_table_styles([dict(selector="th", props=[("text-align", "center")])])
        .set_properties(**{"text-align": "center"})
    )
    return df_styled

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