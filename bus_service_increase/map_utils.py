"""
Functions to create choropleth maps.
# https://stackoverflow.com/questions/64792041/white-gap-between-python-folium-map-and-jupyter-notebook-cell
# Folium examples: https://nbviewer.jupyter.org/github/python-visualization/folium/blob/master/examples/Colormaps.ipynb
# https://nbviewer.jupyter.org/github/python-visualization/folium/blob/master/examples/GeoJsonPopupAndTooltip.ipynb
# https://medium.com/analytics-vidhya/create-and-visualize-choropleth-map-with-folium-269d3fd12fa0
# https://stackoverflow.com/questions/47846744/create-an-asymmetric-colormap
"""
import altair
import folium
from folium.features import GeoJsonPopup, GeoJsonTooltip
from branca.element import Figure

    
# Centroids for various regions and zoom level
REGION_CENTROIDS = {
    "Alameda":  [[37.84, -122.27], 12],
    "Los Angeles": [[34.00, -118.18], 11],
    "CA": [[35.8, -119.4], 6],
}


def make_choropleth_map(df, plot_col, 
                        popup_dict, tooltip_dict, colorscale, 
                        fig_width, fig_height, 
                        zoom=REGION_CENTROIDS["CA"][1], 
                        centroid = REGION_CENTROIDS["CA"][0], 
                        title="Chart Title",
                       ):
    
    fig = Figure(width = fig_width, height = fig_height)
    
    m = folium.Map(location=centroid, tiles='cartodbpositron', 
               zoom_start=zoom, width=fig_width,height=fig_height)

    title_html = f'''
             <h3 align="center" style="font-size:20px"><b>{title}</b></h3>
             '''
    
    fig.get_root().html.add_child(folium.Element(title_html))
        
    TOOLTIP_KWARGS = {
        "min_width": 50,
        "max_width": 100,
        "font_size": "12px",
    }
    
    popup = GeoJsonPopup(
        fields=list(popup_dict.keys()),
        aliases=list(popup_dict.values()),
        localize=True,
        labels=True,
        style=f"background-color: light_gray;",
        min_width = TOOLTIP_KWARGS["min_width"],
        max_width = TOOLTIP_KWARGS["max_width"],
    )

    tooltip = GeoJsonTooltip(
        fields=list(tooltip_dict.keys()),
        aliases=list(tooltip_dict.values()),
        # localize = True sets the commas for numbers, but zipcode should be displayed as string
        #localize=True,
        sticky=False,
        labels=True,
        style=f"""
            background-color: "gray";
            border: 0px #FFFFFF;
            border-radius: 0px;
            box-shadow: 0px;
            font-size: {TOOLTIP_KWARGS["font_size"]};
        """,
        min_width=TOOLTIP_KWARGS["min_width"],
        max_width=TOOLTIP_KWARGS["max_width"],
    )

    #https://medium.com/analytics-vidhya/create-and-visualize-choropleth-map-with-folium-269d3fd12fa0
    g = folium.GeoJson(
        df,
        style_function=lambda x: {
            "fillColor": colorscale(x["properties"][plot_col])
            if x["properties"][plot_col] is not None
            else f"gray",
            "color": "#FFFFFF",
            "fillOpacity": 0.8,
            "weight": 0.2,
        },
        tooltip=tooltip,
        #popup=popup,
    ).add_to(m)
    
    colorscale.caption = "Legend"
    colorscale.add_to(m)
    
    fig.add_child(m)
    
    return fig


#--------------------------------------------------------------#
# Fivethirtyeight altair theme
#--------------------------------------------------------------#
FIVETHIRTYEIGHT_CATEGORY_COLORS = [
  '#30a2da',
  '#fc4f30',
  '#e5ae38',
  '#6d904f',
  '#8b8b8b',
  '#b96db8',
  '#ff9e27',
  '#56cc60',
  '#52d2ca',
  '#52689e',
  '#545454',
  '#9fe4f8',
]

#--------------------------------------------------------------#
# Chart parameters for altair
#--------------------------------------------------------------#
font_size = 16
chart_width = 400
chart_height = 250

markColor = '#30a2da'
axisColor = '#cbcbcb'
guideLabelColor = '#999'
guideTitleColor = '#333'
blackTitle = '#333'
font="Arial"

# Let's add in more top-level chart configuratinos
# But, we can always adjust elements by adjusting chart_width or chart_height in a notebook
# Need to add more since altair_saver will lose a lot of the theme applied
# https://github.com/vega/vega-themes/blob/master/src/theme-fivethirtyeight.ts
def preset_chart_config(chart):
    chart = (chart.properties(
                width = chart_width, height = chart_height,
            ).configure(background="white", font=font)
             .configure_axis(
                 domainColor=axisColor, grid=True,
                 gridColor=axisColor, gridWidth=1,
                 labelColor=guideLabelColor, labelFont=font, labelFontSize=10,
                 titleColor=guideTitleColor, titleFont=font, tickColor=axisColor, 
                 tickSize=10,titleFontSize=14, titlePadding=10,labelPadding=4,
             ).configure_axisBand(grid=False)
             .configure_title(font=font, fontSize=font_size, anchor='middle',
                              fontWeight=300, offset=20,)
             .configure_header(labelFont=font, titleFont=font)
             .configure_legend(labelColor=blackTitle, labelFont=font, labelFontSize=11,
                               padding=1,symbolSize=30,symbolType= 'square',
                               titleColor=blackTitle, titleFont=font, titleFontSize=14, titlePadding=10,)
            )
    return chart