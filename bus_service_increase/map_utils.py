"""
Functions to create choropleth maps.
# https://stackoverflow.com/questions/64792041/white-gap-between-python-folium-map-and-jupyter-notebook-cell
# Folium examples: https://nbviewer.jupyter.org/github/python-visualization/folium/blob/master/examples/Colormaps.ipynb
# https://nbviewer.jupyter.org/github/python-visualization/folium/blob/master/examples/GeoJsonPopupAndTooltip.ipynb
# https://medium.com/analytics-vidhya/create-and-visualize-choropleth-map-with-folium-269d3fd12fa0
# https://stackoverflow.com/questions/47846744/create-an-asymmetric-colormap
"""
import folium
from folium.features import GeoJsonPopup, GeoJsonTooltip
from branca.element import Figure

def make_choropleth_map(df, plot_col, 
                        popup_dict, tooltip_dict, colorscale, 
                        fig_width, fig_height, 
                        zoom=6, centroid = [36.2, -119.1]):
    fig = Figure(width = fig_width, height = fig_height)
    
    m = folium.Map(location=centroid, tiles='cartodbpositron', 
               zoom_start=zoom, width=fig_width,height=fig_height)


    popup = GeoJsonPopup(
        fields=list(popup_dict.keys()),
        aliases=list(popup_dict.values()),
        localize=True,
        labels=True,
        style=f"background-color: light_gray;",
        max_width = 100,
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
            font-size: 12px;
        """,
        max_width=300,
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
        popup=popup,
    ).add_to(m)


    fig.add_child(m)
    
    return fig