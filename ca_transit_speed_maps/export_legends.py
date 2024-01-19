import os
os.environ['USE_PYGEOS'] = '0'

from shared_utils.rt_utils import *
from calitp_data_analysis import get_fs

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

# export speed legend
speeds_legend = add_lines_header(ZERO_THIRTY_COLORSCALE._repr_html_())
speeds_legend = add_inner_labels_caption(speeds_legend, [6, 12, 18, 24], 100, ZERO_THIRTY_COLORSCALE.caption)

fs = get_fs()
path =  f'calitp-map-tiles/speeds_legend.svg'
with fs.open(path, 'w') as writer:  # write out to public-facing GCS?
    writer.write(speeds_legend)
print(f'speed legend written to {path}')

# export variance legend
variance_legend = add_lines_header(VARIANCE_FIXED_COLORSCALE._repr_html_())
variance_legend = add_inner_labels_caption(variance_legend, VARIANCE_RANGE[1:-1], 100, VARIANCE_FIXED_COLORSCALE.caption)

path =  f'calitp-map-tiles/variance_legend.svg'
with fs.open(path, 'w') as writer:  # write out to public-facing GCS?
    writer.write(variance_legend)
print(f'variance legend written to {path}')