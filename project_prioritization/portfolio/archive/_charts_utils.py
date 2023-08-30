# Style a df
from IPython.display import HTML, Image, Markdown, display, display_html

# Charts
from shared_utils import calitp_color_palette as cp
import altair as alt

"""
Chart & Styling Functions
"""
# Preset to make chart 25% smaller than shared_utils
# Since the webpage is kind of small
chart_width = 400
chart_height = 250

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

#Basic Bar Chart
def basic_bar_chart_custom_tooltip(df, x_col, y_col, tooltip_col, colorcol, chart_title=''):
    if chart_title == "":
        chart_title = (f"{labeling(x_col)} by {labeling(y_col)}")
    chart = (alt.Chart(df)
             .mark_bar()
             .encode(
                 x=alt.X(x_col, title=labeling(x_col)),
                 y=alt.Y(y_col, title=labeling(y_col),sort=('-x')),
                 color = alt.Color(colorcol, 
                                  scale=alt.Scale(
                                      range=cp.CALITP_DIVERGING_COLORS),
                                      legend=alt.Legend(title=(labeling(colorcol)))
                                  ),
                tooltip = [tooltip_col])
             .properties( 
                       title=chart_title)
    )
    chart = preset_chart_config(chart)

    return chart

# An interactive dual bar chart
def dual_bar_chart(df, control_field:str, chart1_nominal:str,
                  chart1_quant: str, chart2_nominal:str, 
                  chart2_quant:str, chart1_tooltip_cols: list,
                  chart2_tooltip_cols: list, chart_title: str):
    """An interactive dual bar chart
    https://stackoverflow.com/questions/53404826/how-to-link-two-bar-charts-in-altair
    
    Args:
        Universal args
        df: the dataframe
        control_field (str): the field of the 1st chart that controls your 2nd chart.
        
        Chart 1 Args
        chart1_nominal (str): nominal column for 1st bar chart. should be in format 'column_name:N'
        chart1_quant (str): quantitive column for 1st bar chart. should be in format 'column_name:Q'
        
        Chart 2 Args
        chart2_nominal (str): nominal column for 2nd bar chart. should be in format 'column_name:N'
        chart2_quant (str): quantitive column for 2nd bar chart. should be in format 'column_name:Q'
        
        Tooltips
        chart1_tooltip_cols (list): list of columns to place in tooltip in chart 1
        chart2_tooltip_cols (list): list of columns to place in tooltip chart 2
    Returns:
        Returns two vertically concated bar charts: the first bar chart controls the second bar chart.
    """
    # Column that controls the bar charts
    category_selector = alt.selection_multi(fields=[control_field])
    
    # Build first chart
    chart1 = (alt.Chart(df).mark_bar().encode(
        x=alt.X(chart1_quant),
        y=alt.Y(chart1_nominal), 
        color = alt.Color(chart1_nominal, scale=alt.Scale(
        range=cp.CALITP_DIVERGING_COLORS), legend = None),
        tooltip = chart1_tooltip_cols)
        .properties(title=chart_title)
        .add_selection(category_selector))
    
    # Build second chart
    chart2 = (alt.Chart(df).mark_bar().encode(
        x=alt.X(chart2_quant),
        y=alt.Y(chart2_nominal), 
        color = alt.Color(chart2_nominal, scale=alt.Scale(
        range=cp.CALITP_DIVERGING_COLORS), legend = None),
        tooltip = chart2_tooltip_cols)
        .transform_filter(category_selector))
    
    chart1 = preset_chart_config(chart1)
    chart2 = preset_chart_config(chart2)
    return(chart1 & chart2)

# A basic pie chart
def basic_pie_chart(df, quant_col:str, nominal_col:str, label_col:str,
                   chart_title:str):
    """
    quant_col (str): format as  "Column Name:Q"
    nominal_col (str):  format as "Column Name:N"
    label_col (str): format as "Column Name:N"
    """
    # Base Chart
    base = (alt.Chart(df)
            .encode(theta=alt.Theta(quant_col, stack=True), 
            color=alt.Color(nominal_col, 
            scale = alt.Scale(range = cp.CALITP_DIVERGING_COLORS),
            legend = alt.Legend(title=labeling(label_col))),
            tooltip = [label_col,quant_col])
            .properties(title=chart_title)
           )
    # Set size of the pie 
    pie = base.mark_arc(outerRadius=80)
    
    # Add labels
    text = base.mark_text(radius=90, size=10).encode(text=label_col)
    
    # Combine the chart and labels
    chart =  preset_chart_config(pie + text)
    
    return chart

# 2 charts that are controlled by a dropdown menu.
def dual_chart_with_dropdown(
    df,
    dropdown_list: list,
    dropdown_field: str,
    x_axis_chart1: str,
    y_axis_chart1: str,
    color_col1: str,
    chart1_tooltip_cols: list,
    x_axis_chart2: str,
    y_axis_chart2: str,
    color_col2: str,
    chart2_tooltip_cols: list,
    chart_title: str,
):
    """Two bar charts controlled by a dropdown
    Args:
        Universal args
        df: the dataframe
        dropdown_list(list): a list of all the values in the dropdown menu,
        dropdown_field(str): column where the dropdown menu's values are drawn from,
        
        Chart 1 Args
        x_axis_chart1(str): x axis value for chart 1 - encode as Q or N,
        y_axis_chart1(str): y axis value for chart 1 - encode as Q or N,
        color_col1(str): column to color the graphs for chart 1,
        chart1_tooltip_cols(list): list of all the columns to populate the tooltip,
        
        Chart 2 Args 
        x_axis_chart2(str): x axis value for chart 2 - encode as Q or N,
        y_axis_chart2(str): x axis value for chart 2 - encode as Q or N,
        color_col2(str): column to color the graphs for chart 2,
        chart2_tooltip_cols(list): list of all the columns to populate the tooltip,
        
        Chart Title
        chart_title(str):chart title,
    Returns:
        Returns two  bar charts that are controlled by a dropdown
    """
    # Create drop down menu
    input_dropdown = alt.binding_select(options=dropdown_list, name="Select ")

    # The column tied to the drop down menu
    selection = alt.selection_single(fields=[dropdown_field], bind=input_dropdown)

    chart1 = (
        alt.Chart(df)
        .mark_bar()
        .encode(
            x=x_axis_chart1,
            y=(y_axis_chart1),
            color=alt.Color(
                color_col1, scale=alt.Scale(range=cp.CALITP_DIVERGING_COLORS)
                , legend = None
            ),
            tooltip=chart1_tooltip_cols,
        )
        .properties(title=chart_title)
        .add_selection(selection)
        .transform_filter(selection)
    )

    chart2 = (
        alt.Chart(df)
        .mark_bar()
        .encode(
            x=x_axis_chart2,
            y=(y_axis_chart2),
            color=alt.Color(
                color_col2, scale=alt.Scale(range=cp.CALITP_DIVERGING_COLORS)
                , legend = None
            ),
            tooltip=chart2_tooltip_cols,
        )
        .add_selection(selection)
        .transform_filter(selection)
    )
    chart1 = preset_chart_config(chart1)
    chart2 = preset_chart_config(chart2)
    return chart1 | chart2


# Create 3 charts
def repeated_charts(
    df,
    color_col: str,
    y_encoding_list: list,
    x_encoding_list: list,
    chart_title: str,
    tooltip_col: list,
):
    base = (
        alt.Chart()
        .mark_bar()
        .encode(
            color=alt.Color(
                color_col, scale=alt.Scale(range=cp.CALITP_DIVERGING_COLORS), 
                legend = None
            ),
            tooltip= y_encoding_list + tooltip_col,
        )
        .properties(width=150, height=100)
        .interactive()
    )

    chart = alt.vconcat(data=df)
    for y_encoding in y_encoding_list:
        row = alt.hconcat()
        for x_encoding in x_encoding_list:
            row |= base.encode(x=x_encoding, y=y_encoding)
        chart &= row

    return chart.properties(title=chart_title)


"""
Style the dataframe by removing the index and gray banding,
dropping certain columns, and centering text. Adds scrollbar
and a maximum height & width.
"""
def styled_df(df):
    display(
    HTML(
        "<div style='height: 300px; overflow: auto; width: 1000px'>"
        + (
            (df)
            .style.set_properties(**{"background-color": "white", "font-size": "10pt",})
            .set_table_styles([dict(selector="th", props=[("text-align", "center")])])
            .set_properties(**{"text-align": "center"})
            .hide(axis="index")
            .render()
        )
        + "</div>"
    ))
