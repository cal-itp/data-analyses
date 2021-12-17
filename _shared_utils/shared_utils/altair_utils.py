import altair as alt

#--------------------------------------------------------------#
# Fivethirtyeight altair theme
#--------------------------------------------------------------#
FIVETHIRTYEIGHT_CATEGORY_COLORS = [
  '#30a2da', '#fc4f30', '#e5ae38',
  '#6d904f', '#8b8b8b', '#b96db8',
  '#ff9e27', '#56cc60', '#52d2ca',
  '#52689e', '#545454', '#9fe4f8',
]

FIVETHIRTYEIGHT_DIVERGING_COLORS = [
    '#cc0020', '#e77866', '#f6e7e1', 
    '#d6e8ed', '#91bfd9', '#1d78b5'
]
FIVETHIRTYEIGHT_SEQUENTIAL_COLORS = [
    '#d6e8ed', '#cee0e5', '#91bfd9', 
    '#549cc6', '#1d78b5'
]

#--------------------------------------------------------------#
# Chart parameters for altair
#--------------------------------------------------------------#
font_size = 16
chart_width = 400
chart_height = 250

markColor = '#30a2da'
axisColor = '#cbcbcb'
guideLabelColor = '#474747'
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
                               titleColor=blackTitle, titleFont=font, titleFontSize=14,
                               titlePadding=10,
                               labelLimit=0
                              )
            )
    return chart