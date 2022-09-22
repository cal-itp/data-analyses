import altair as alt
import altair_saver
from shared_utils import geography_utils
from shared_utils import calitp_color_palette as cp
# from shared_utils import styleguide
from shared_utils import altair_utils

'''
Other Functions
'''
#Grab value counts and turn it into a dataframe
def value_counts_df(df, col_of_interest):
    df = (
    df[col_of_interest]
    .value_counts()
    .to_frame()
    .reset_index()
    )
    return df 

'''
Crosswalks
'''
# Adjust CT district to full names
full_ct_district = {
    7: "District 7: Los Angeles",
    4: "District 4: Bay Area / Oakland",
    "VAR": "Various",
    10: "District 10: Stockton",
    11: "District 11: San Diego",
    3: "District 3: Marysville / Sacramento",
    12: "District 12: Orange County",
    8: "District 8: San Bernardino / Riverside",
    5: "District 5: San Luis Obispo / Santa Barbara",
    6: "District 6: Fresno / Bakersfield",
    1: "District 1: Eureka",
}

'''
Chart Functions
'''
chart_width = 400
chart_height = 250

def preset_chart_config(chart: alt.Chart) -> alt.Chart:
    chart = chart.properties(
        width=chart_width,
        height=chart_height,
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

# Bar chart with interactive tooltip: x_col and y_col will show up 
def basic_bar_chart_custom_tooltip(df, x_col, y_col, tooltip_col, colorcol, chart_title=''):
    if chart_title == "":
        chart_title = (f"{labeling(x_col)} by {labeling(y_col)}")
    chart = (alt.Chart(df)
             .mark_bar()
             .encode(
                 x=alt.X(x_col, title=labeling(x_col), ),
                 y=alt.Y(y_col, title=labeling(y_col),sort=('-x')),
                 color = alt.Color(colorcol, 
                                  scale=alt.Scale(
                                      range=cp.CALITP_CATEGORY_BRIGHT_COLORS),
                                      legend=alt.Legend(title=(labeling(colorcol)))
                                  ),
                tooltip = [x_col, tooltip_col])
             .properties( 
                       title=chart_title)
    )

    chart= preset_chart_config(chart)
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
        Returns two horizontally concated bar charts. The first bar chart controls the second bar chart.
    """
    # Column that controls the bar charts
    category_selector = alt.selection_multi(fields=[control_field])
    
    # Build Chart one
    chart1 = (alt.Chart(df).mark_bar().encode(
        x=alt.X(chart1_quant),
        y=alt.Y(chart1_nominal), 
        color = alt.Color(chart1_quant, scale=alt.Scale(
        range=cp.CALITP_CATEGORY_BRIGHT_COLORS), legend = None),
        tooltip = [chart1_nominal, chart1_quant,])
        .add_selection(category_selector))

    chart2 = (alt.Chart(df).mark_bar().encode(
        x=alt.X(chart2_quant),
        y=alt.Y(chart2_nominal), 
        color = alt.Color(chart2_quant, scale=alt.Scale(
        range=cp.CALITP_CATEGORY_BRIGHT_COLORS), legend = None),
        tooltip = [chart2_nominal,chart2_quant,])
        .transform_filter(category_selector))

    return(chart1 | chart2)