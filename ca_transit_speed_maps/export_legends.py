from shared_utils.rt_utils import *
from shared_utils.webmap_utils import export_legend
from calitp_data_analysis import get_fs

#  utility script for saving branca colormaps as static svg files for the map app to pick up and display

if __name__ == "__main__":
    
    fs = get_fs()
    export_legend(cmap=ZERO_THIRTY_COLORSCALE, filename='speeds_legend.svg', inner_labels=[6, 12, 18, 24])
    export_legend(cmap=VARIANCE_FIXED_COLORSCALE, filename='variance_legend.svg', inner_labels=VARIANCE_RANGE[1:-1])
    export_legend(cmap=ACCESS_ZERO_THIRTY_COLORSCALE, filename='speeds_legend_color_access.svg', inner_labels=[6, 12, 18, 24])