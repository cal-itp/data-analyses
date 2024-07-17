from shared_utils.rt_utils import *
from calitp_data_analysis import get_fs

#  utility script for saving branca colormaps as static svg files for the map app to pick up and display
#  could be moved to shared_utils or as part of future map app refactor efforts?

def add_lines_header(svg):

    # add newlines
    svg = svg.replace('<', '\n<')

    # svg from _repr_html_ missing this apperently essential header
    svg_header = \
    '''<?xml version="1.0" standalone="no"?>
    <!DOCTYPE svg PUBLIC "-//W3C//DTD SVG 20010904//EN"
     "http://www.w3.org/TR/2001/REC-SVG-20010904/DTD/svg10.dtd">
     <svg version="1.0" xmlns="http://www.w3.org/2000/svg" height="60" width="500"
     viewBox ="0 0 500 60">
    '''
    # strip first svg tag from original string (already in header)
    svg_strip_tag = svg.split('width="500">\n')[1]
    export_svg = svg_header + svg_strip_tag
    return export_svg

def add_inner_labels_caption(svg, labels, spacing, caption):
    labels = [round(n, 1) for n in list(labels)]
    inner_labels_str = ''.join(
        [f'<text x="{spacing + 100 * labels.index(label)}" y="35" style="text-anchor:end;">{label}\n</text>' for label in labels]
    )
    
    inner_labels_str += f'<text x="0" y="50">{caption}\n</text>'
    inner_labels_str += '\n</svg>'
    export_svg = svg.replace('\n</svg>', inner_labels_str)
    
    return export_svg

def export_legend(cmap:branca.colormap.StepColormap, filename):
    
    legend = add_lines_header(cmap._repr_html_())
    legend = add_inner_labels_caption(legend, [6, 12, 18, 24], 100, cmap.caption)

    path =  f'calitp-map-tiles/{filename}'
    with fs.open(path, 'w') as writer:  # write out to public-facing GCS?
        writer.write(legend)
    print(f'legend written to {path}')

if __name__ == "__main__":
    
    fs = get_fs()
    export_legend(ZERO_THIRTY_COLORSCALE, 'speeds_legend.svg')
    export_legend(VARIANCE_FIXED_COLORSCALE, 'variance_legend.svg')
    export_legend(ACCESS_ZERO_THIRTY_COLORSCALE, 'speeds_legend_color_access.svg')