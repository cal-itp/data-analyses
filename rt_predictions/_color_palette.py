"""
Define color palette.

Use https://www.color-name.com/hex/ to get
closest color names based on hex number.

great_tables colors:
https://github.com/posit-dev/great-tables/tree/main/great_tables/_data_color/constants.py
altair colors:
https://github.com/vega/altair/blob/main/altair/vegalite/v6/schema/_typing.py#L333
"""

COLOR_NAME_TO_HEX = {
    "blueberry": "#5b8efd",
    "lady_blue": "#765fec",  # purple
    "light_cadmium_yellow": "#fcb40e",
    "electric_orange": "#fc5c04",
    "vivid_cerise": "#dd217d",  # dark pink
    "earls_green": "#ccbb44",  # mustard yellow
    "happy_red": "#ee6677",
    "aquatic": "#66ccee",
    "metro_blue": "#4477aa",
    "light_gray": "#faf0e6",  # linen in gt
    "valentino": "#7a378b",  # dark purple, mediumorchid4 in gt
    "lizard_green": "#6e8b3d",  # dark olive, darkolivegreen4 in gt
}


def get_color(color_name: str) -> str:
    return COLOR_NAME_TO_HEX[color_name]


FULL_CATEGORICAL_COLORS = [
    COLOR_NAME_TO_HEX[c]
    for c in ["blueberry", "lady_blue", "light_cadmium_yellow", "electric_orange", "vivid_cerise", "earls_green"]
]

TRI_COLORS = [COLOR_NAME_TO_HEX[c] for c in ["earls_green", "blueberry", "vivid_cerise"]]
FOUR_COLORS = [COLOR_NAME_TO_HEX[c] for c in ["vivid_cerise", "light_cadmium_yellow", "earls_green", "blueberry"]]

FOUR_COLORS2 = [COLOR_NAME_TO_HEX[c] for c in ["happy_red", "aquatic", "earls_green", "metro_blue"]]

# Define the colors for prediction_error_label
PREDICTION_ERROR_COLOR_PALETTE = {
    "3-5 min late": get_color("vivid_cerise"),
    "1-3 min late": get_color("electric_orange"),
    "1 min early to 1 min late": get_color("light_cadmium_yellow"),
    "1-3 min early": get_color("aquatic"),
    "3-5 min early": get_color("metro_blue"),
}
