import pandas as pd

# Geography
from shared_utils import geography_utils
import geopandas as gpd

# Charts
from shared_utils import calitp_color_palette as cp
from shared_utils import altair_utils
import altair as alt

# Format
from babel.numbers import format_currency

# Style a df
from IPython.display import HTML, Image, Markdown, display, display_html

GCS_FILE_PATH = "gs://calitp-analytics-data/data-analyses/project_prioritization/"

# Wordcloud
import matplotlib.pyplot as plt  # plot package
import seaborn as sns  # statist graph package
import wordcloud  # will use for the word cloud plot
from wordcloud import (  # optional to filter out the stopwords
    STOPWORDS,
    ImageColorGenerator,
    WordCloud,
)

# Strings
import re
from collections import Counter
from nltk import ngrams
from nltk.corpus import stopwords
from nltk.tokenize import sent_tokenize, word_tokenize

# Saving Geojson
from calitp import *
from calitp.storage import get_fs
fs = get_fs()
import os

"""
Chart Functions
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
Other Functions
"""
# Export a GDF as a geojson to GCS
def geojson_gcs_export(gdf, GCS_FILE_PATH, FILE_NAME):
    """
    Save geodataframe as parquet locally,
    then move to GCS bucket and delete local file.

    gdf: geopandas.GeoDataFrame
    GCS_FILE_PATH: str. Ex: gs://calitp-analytics-data/data-analyses/my-folder/
    FILE_NAME: str. Filename.
    """
    gdf.to_file(f"./{FILE_NAME}.geojson", driver="GeoJSON")
    fs.put(f"./{FILE_NAME}.geojson", f"{GCS_FILE_PATH}{FILE_NAME}.geojson")
    os.remove(f"./{FILE_NAME}.geojson")

    
# Categorize a project by percentiles of whatever column you want
def project_size_rating(dataframe, original_column: str, new_column: str):
    """Rate a project by percentiles and returning percentiles for any column
    
    Args:
        dataframe
        original_column (str): column to create the metric off of
        new_column (str): new column to hold results
    Returns:
        the dataframe with the new column with the categorization.
    """
    # Get percentiles 
    p75 = dataframe[original_column].quantile(0.75).astype(float)
    p25 = dataframe[original_column].quantile(0.25).astype(float)
    p50 = dataframe[original_column].quantile(0.50).astype(float)

    # Function for fleet size
    def project_size(row):
        if (row[original_column] > 0) and (row[original_column] <= p25):
            return "25th percentile"
        elif (row[original_column] > p25) and (row[original_column] <= p75):
            return "50th percentile"
        elif row[original_column] > p75:
            return "75th percentile"
        else:
            return "No Info"

    dataframe[new_column] = dataframe.apply(lambda x: project_size(x), axis=1)

    return dataframe

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
    df.columns = df.columns.str.replace("_", " ").str.strip().str.title()
    return df

# Generate wordcloud
# From: https://www.kaggle.com/code/olgaberezovsky/word-cloud-using-python-pandas/notebook
def wordcloud(df, desc_column: str, max_words: int, additional_stop_words:list):
    
    wordstring = " ".join(df[desc_column].str.lower())
    
    plt.figure(figsize=(15, 15))
    wc = WordCloud(
    background_color="white",
    stopwords=STOPWORDS.update(additional_stop_words),
    max_words=50,
    max_font_size=200,
    width=650,
    height=650,)
    
    wc.generate(wordstring)
    # plt.imshow(wc.recolor(colormap="tab10", random_state=30), interpolation="bilinear")
    plt.axis("off")
    
    return plt.imshow(wc.recolor(colormap="tab10", random_state=30), interpolation="bilinear")

# Grab a list of all the string that appear in a column
# Natalie's function
def get_list_of_words(df, col: str, additional_words_to_remove: list):

    # get just the one col
    column = df[[col]]
    # remove single-dimensional entries from the shape of an array
    col_text = column.squeeze()
    # get list of words
    text_list = col_text.tolist()

    # Join all the column into one large text blob, lower text
    text_list = " ".join(text_list).lower()

    # remove punctuation
    text_list = re.sub(r"[^\w\s]", "", text_list)

    # List of stopwords
    swords = [re.sub(r"[^A-z\s]", "", sword) for sword in stopwords.words("english")]

    # Append additionally words to remove from results
    swords.extend(additional_words_to_remove)

    # Remove stopwords
    clean_text_list = [
        word for word in word_tokenize(text_list.lower()) if word not in swords
    ]

    return clean_text_list

"""
After using the function get_list_of_words() to return a cleaned 
list of text, find the most common phrases that pop up in 
the projects' descriptions.
"""
def common_phrases(df, clean_text_list: list, phrase_length: int):

    c = Counter(
        [" ".join(y) for x in [phrase_length] for y in ngrams(clean_text_list, x)]
    )
    df = pd.DataFrame({"phrases": list(c.keys()), "total": list(c.values())})
    
    # Sort by most common phrases to least
    df = df.sort_values("total", ascending=False)
    
    # Filter out any phrases with less than 2 occurences
    df = (df.loc[df["total"] > 1]).reset_index()
    
    return df
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

"""
Merge a dataframe with Caltrans District Map 
to return a gdf
"""
Caltrans_shape = "https://gis.data.ca.gov/datasets/0144574f750f4ccc88749004aca6eb0c_0.geojson?outSR=%7B%22latestWkid%22%3A3857%2C%22wkid%22%3A102100%7D"

def create_caltrans_map(df):
    
    # Load in Caltrans shape
    ct_geojson = gpd.read_file(f"{Caltrans_shape}").to_crs(epsg=4326)
   
    # Inner merge 
    districts_gdf = ct_geojson.merge(
    df, how="inner", left_on="DISTRICT", right_on="district_number")
    
    return districts_gdf

"""
Merge a dataframe with county geography
to return a gdf
"""
ca_gdf = "https://opendata.arcgis.com/datasets/8713ced9b78a4abb97dc130a691a8695_0.geojson"

def create_county_map(df, left_df_merge_col:str, 
                      right_df_merge_col:str):
    
    # Load in Caltrans shape
    county_geojson = gpd.read_file(f"{ca_gdf}").to_crs(epsg=4326)
    
    # Keep only the columns we want
    county_geojson = county_geojson[['COUNTY_NAME', 'COUNTY_ABBREV','geometry']]
    
    # Replace abbreviations to Non SHOPP
    county_geojson["COUNTY_ABBREV"] = county_geojson["COUNTY_ABBREV"].replace(
        {
        "LOS": "LA",
        "DEL": "DN",
        "SFO": "SF",
        "SMT": "SM",
        "MNT": "MON",
        "SDG": "SD",
        "CON": "CC",
        "SCZ": "SCR",
        "SJQ": "SJ",
        "SBA": "SB",
    }
    )
    
    # Inner merge 
    county_df = county_geojson.merge(
    df, how="inner", left_on=left_df_merge_col, right_on=right_df_merge_col)

    return county_df

"""
Functions specific to this project
"""
# Create a fake scorecard
def create_fake_score_card(df):
    # Subset 
    df2 = df[
    [
        "project_name",
        "increase_peak_person_throughput",
        "reduction_in_peak_period_delay",
        "reduction_in_fatal_and_injury_crashes",
        "reduction_in_injury_rates",
        "increase_access_to_jobs",
        "increase_access_jobs_to_DAC",
        "commercial_dev_developed",
        "tons_of_goods_impacted",
        "improve_air_quality",
        "impact_natural_resources",
        "support_of_trasnportation",
       
    ]]
    
    # Melt
    df2 = pd.melt(
    df2,
    id_vars=["project_name"],
    value_vars=[
        "increase_peak_person_throughput",
        "reduction_in_peak_period_delay",
        "reduction_in_fatal_and_injury_crashes",
        "reduction_in_injury_rates",
        "increase_access_to_jobs",
        "increase_access_jobs_to_DAC",
        "commercial_dev_developed",
        "tons_of_goods_impacted",
        "improve_air_quality",
        "impact_natural_resources",
        "support_of_trasnportation",
    ])
    
    # Remove underscores off of old column names
    df2["variable"] = df2["variable"].str.replace("_", " ").str.title()
    
    # New column with broader Measures
    df2["Category"] = df2["variable"]

    df2["Category"] = df2["Category"].replace(
    {
        "Increase Peak Person Throughput": "Congestion Mitigation",
        "Reduction In Peak Period Delay": "Congestion Mitigation",
        "Reduction In Fatal And Injury Crashes": "Safety",
        "Reduction In Injury Rates": "Safety",
        "Increase Access To Jobs": "Accessibility Increase",
        "Increase Access Jobs To Dac": "Accessibility Increase",
        "Commercial Dev Developed": "Economic Dev.",
        "Tons Of Goods Impacted": "Economic Dev.",
        "Improve Air Quality": "Environment",
        "Impact Natural Resources": "Environment",
        "Support Of Trasnportation": "Land Use",
    })
    
    # Get total scores
    total = (
    df2.groupby(["project_name", "Category"])
    .agg({"value": "sum"})
    .rename(columns={"value": "Total Category Score"})
    .reset_index())
    
    # Merge
    df2 = pd.merge(
    df2, total, how="left", on=["project_name", "Category"])
    
    # Add fake descriptions
    for i in ["Measure Description",
              "Factor Weight",
              "Weighted Factor Value",
              "Category Description",]:
        df2[i] = "Text Here"
    
    # Second subset
    df3 = df[["total_project_cost__$1,000_",
        "total_unfunded_need__$1,000_",
         "project_name",
        "project_description"]]
    
    # Melt
    df3 = pd.melt(
    df3,
    id_vars=["project_name","project_description",],
    value_vars=[
        "total_project_cost__$1,000_",
        "total_unfunded_need__$1,000_",
    ])
    
    # Change names
    df3 = df3.rename(columns = {'variable':'monetary',
                                'values':'monetary   values'})
    
     # Final Merge
    final = pd.merge(
    df2, df3, how="inner",  on = ["project_name"])
    
    # Remove underscores off of old column names
    final["monetary"] = final["monetary"].str.replace("_", "  ").str.title()
    return final
        

"""
Create summary table: returns total projects, total cost,
and money requested by the column of your choice. 
"""
def summarize_by_project_names(df, col_wanted: str):
    """
    df: original dataframe to summarize
    col_wanted: to column to groupby
    """
    df = (
        df.groupby([col_wanted])
        .agg({"Project Name": "count", 
              "Total Project Cost  $1,000": "sum",
              "Total Unfunded Need  $1,000":"sum"})
        .reset_index()
        .sort_values("Project Name", ascending=False)
        .rename(columns={"Project Name": "Total Projects"})
    )

    df = df.reset_index(drop=True)

    # Create a formatted monetary col
    df["Total Project Cost  $1,000"] = df["Total Project Cost  $1,000"].apply(
        lambda x: format_currency(x, currency="USD", locale="en_US")
    )

    # Create a formatted monetary col
    df["Total Unfunded Need  $1,000"] = df["Total Unfunded Need  $1,000"].apply(
        lambda x: format_currency(x, currency="USD", locale="en_US")
    )
    # Clean up column names, remove snakecase
    df = clean_up_columns(df)

    return df

"""
Concat summary stats of 
parameter  county & parameter district into one dataframe 
"""
def county_district_comparison(df_parameter_county, df_parameter_district):
    # Grab the full district name
    district_full_name = df_parameter_district["district_full_name"][0]

    # Grab the full county name
    county_full_name = df_parameter_county["full_county_name"][0]

    # Create summary table for district
    district =  summarize_by_project_names(df_parameter_district, "primary_mode")

    # Create summary table for county
    county = summarize_by_project_names(df_parameter_county, "primary_mode")

    # Append grand total and keep only that row...doesn't work when I try to do this with a for loop
    district = (
        district.append(district.sum(numeric_only=True), ignore_index=True)
        .tail(1)
        .reset_index(drop=True)
    )
    county = (
        county.append(county.sum(numeric_only=True), ignore_index=True)
        .tail(1)
        .reset_index(drop=True)
    )

    # Concat
    concat1 = pd.concat([district, county]).reset_index(drop=True)

    # Declare a list that is to be converted into a column
    geography = [district_full_name, county_full_name]
    concat1["Geography"] = geography

    # Drop old cols
    concat1 = concat1.drop(
        columns=[
            "Primary Mode",
            "Fake Fund Formatted",
            "Total Project ($1000) Formatted",
        ]
    )

    # Create new formatted monetary cols
    concat1["Total Project ($1000) Formatted"] = concat1[
        "Total Project Cost  $1,000"
    ].apply(lambda x: format_currency(x, currency="USD", locale="en_US"))

    concat1["Fake Fund Formatted"] = concat1["Current Fake Fund Requested"].apply(
        lambda x: format_currency(x, currency="USD", locale="en_US")
    )

    return concat1