"""
Setting altair theme to resemble Urban: 
https://towardsdatascience.com/consistently-beautiful-visualizations-with-altair-themes-c7f9f889602

GH code: 
https://github.com/chekos/altair_themes_blog/tree/master/notebooks

Download Google font: 
https://gist.github.com/ravgeetdhillon/0063aaee240c0cddb12738c232bd8a49
"""

def urban_theme():
    # Typography
    font = "Lato"
    # At Urban it's the same font for all text but it's good to keep them separate in case you want to change one later.
    labelFont = "Lato" 
    sourceFont = "Lato"
    # Axes
    axisColor = "#000000"
    gridColor = "#DEDDDD"
    # Colors
    main_palette = ["#1696d2", 
                    "#d2d2d2",
                    "#000000", 
                    "#fdbf11", 
                    "#ec008b", 
                    "#55b748", 
                    "#5c5859", 
                    "#db2b27", 
                   ]
    sequential_palette = ["#cfe8f3", 
                          "#a2d4ec", 
                          "#73bfe2", 
                          "#46abdb", 
                          "#1696d2", 
                          "#12719e", 
                         ]
return {
        # width and height are configured outside the config dict because they are Chart configurations/properties not chart-elements' configurations/properties.
        "width": 685, # from the guide
        "height": 380, # not in the guide
        "config": {
            "title": {
                "fontSize": 18,
                "font": font,
                "anchor": "start", # equivalent of left-aligned.
                "fontColor": "#000000"
            },
            "axisX": {
                "domain": True,
                "domainColor": axisColor,
                "domainWidth": 1,
                "grid": False,
                "labelFont": labelFont,
                "labelFontSize": 12,
                "labelAngle": 0, 
                "tickColor": axisColor,
                "tickSize": 5, # default, including it just to show you can change it
                "titleFont": font,
                "titleFontSize": 12,
                "titlePadding": 10, # guessing, not specified in styleguide
                "title": "X Axis Title (units)", 
            },
            "axisY": {
                "domain": False,
                "grid": True,
                "gridColor": gridColor,
                "gridWidth": 1,
                "labelFont": labelFont,
                "labelFontSize": 12,
                "labelAngle": 0, 
                "ticks": False, # even if you don't have a "domain" you need to turn these off.
                "titleFont": font,
                "titleFontSize": 12,
                "titlePadding": 10, # guessing, not specified in styleguide
                "title": "Y Axis Title (units)", 
                # titles are by default vertical left of axis so we need to hack this 
                "titleAngle": 0, # horizontal
                "titleY": -10, # move it up
                "titleX": 18, # move it to the right so it aligns with the labels 
            },
            "range": {
                "category": main_palette,
                "diverging": sequential_palette,
            },
            "legend": {
                "labelFont": labelFont,
                "labelFontSize": 12,
                "symbolType": "square", # just 'cause
                "symbolSize": 100, # default
                "titleFont": font,
                "titleFontSize": 12,
                "title": "", # set it to no-title by default
                "orient": "top-left", # so it's right next to the y-axis
                "offset": 0, # literally right next to the y-axis.
            },
            "view": {
                "stroke": "transparent", # altair uses gridlines to box the area where the data is visualized. This takes that off.
            },
            "background": {
                "color": "#FFFFFF", # white rather than transparent
            },
            ### MARKS CONFIGURATIONS ###
            "area": {
               "fill": markColor,
           },
           "line": {
               "color": markColor,
               "stroke": markColor,
               "strokeWidth": 5,
           },
           "trail": {
               "color": markColor,
               "stroke": markColor,
               "strokeWidth": 0,
               "size": 1,
           },
           "path": {
               "stroke": markColor,
               "strokeWidth": 0.5,
           },
           "point": {
               "filled": True,
           },
           "text": {
               "font": sourceFont,
               "color": markColor,
               "fontSize": 11,
               "align": "right",
               "fontWeight": 400,
               "size": 11,
           }, 
           "bar": {
                "size": 40,
                "binSpacing": 1,
                "continuousBandSize": 30,
                "discreteBandSize": 30,
                "fill": markColor,
                "stroke": False,
            },
}
    }


'''
# Run this in notebook
%%html
<style>
@import url('https://fonts.googleapis.com/css?family=Lato');
</style>
'''