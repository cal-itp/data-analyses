import pandas as pd
import geopandas as gpd
from siuba import *
import datetime as dt

from bus_service_utils import better_bus_utils
from shared_utils.rt_utils import ZERO_THIRTY_COLORSCALE
from rt_analysis import rt_filter_map_plot, rt_parser
import folium

def fm_from_bbutils(ct_dist, category, get_sorted = False,
                    fm_dict = {}, analysis_date = dt.date(2022, 5, 4)):
    '''
    ct_dist: str, Caltrans district, formatted as in "11 - San Diego"
    category: str, reccomendation category, "corridor" or "hotspot"
    get_sorted: use better_bus_utils.get_sorted_transit_routes instead of 
    corridor/hotspot dedicated function. Default False, override if few/no
    suitable routes returned.
    fm_dict: dictionary of rt_analysis.rt_filter_map_plot.RtFilterMapper instances,
    if some are already loaded (for example when running for hotspot after corridor)
    '''
    
    if get_sorted:
        bbutil_gdf = better_bus_utils.get_sorted_transit_routes(recommendation_category=category)
    elif category == "corridor":
        bbutil_gdf = better_bus_utils.select_transit_routes_corridor_improvements()
    elif category == "hotspot":
        bbutil_gdf = better_bus_utils.select_transit_routes_hotspot_improvements()
    
    bbutil_gdf = bbutil_gdf >> filter(_.caltrans_district==ct_dist)
    if bbutil_gdf.empty:
        if get_sorted:
            print('no routes at all')
            return
        print(f"{ct_dist} {category} empty with default util, retrying with get_sorted")
        return fm_from_bbutils(ct_dist = ct_dist, category = category, get_sorted = True,
                              fm_dict = fm_dict, analysis_date = analysis_date)
    num_routes = max([20, bbutil_gdf.shape[0] // 5])
    bbutil_gdf = bbutil_gdf >> head(num_routes) # display top 20% or at least 20 routes
    unique_itp_ids = bbutil_gdf.calitp_itp_id.unique()
    mappable = bbutil_gdf >> select(-_.service_date)
    if len(unique_itp_ids) == 1:
        display(mappable.explore("route_id", legend=True, tiles="CartoDB positron"))
    else:
        display(mappable.explore("calitp_itp_id", legend=True, tiles="CartoDB positron"))
    fm_dict_new = {}
    for itp_id in unique_itp_ids:
        print(f'loading filter/mapper for {itp_id}')
        if itp_id in fm_dict.keys():
            fm = fm_dict[itp_id]
        else:
            try:
                fm = rt_filter_map_plot.from_gcs(itp_id=itp_id, analysis_date=analysis_date)
            except:
                print(f"no filtermapper for {itp_id}")
                continue
        rt_id_names = (fm.rt_trips
                 >> distinct(_.route_id, _.route_short_name)
                 >> inner_join(_, bbutil_gdf >> select(_.route_id), on = 'route_id')
                )
        try:
            fm.set_filter(route_names = rt_id_names.route_short_name)
        except:
            print(f"no route filter on {itp_id}")
        fm_dict_new[itp_id] = fm
    print(fm_dict_new)
    return fm_dict_new

def speedmap_from_combined(gdf, district, m = None):
    '''
    Renders speedmaps from combined segment speed gdfs, outside the
    rt_analysis.rt_filter_map_plot.RtFilterMapper class.
    Generally, these gdfs will be concatenated RtFilterMapper.detailed_map_view
    '''
    colorscale = ZERO_THIRTY_COLORSCALE
    size = [900, 550]
    centroid = (gdf.geometry.centroid.y.mean(), gdf.geometry.centroid.x.mean())

    display_cols = ['_20p_mph', 'time_formatted', 'miles_from_last',
                   'route_short_name', 'trips_per_hour', 'shape_id',
                   'stop_sequence']
    display_aliases = ['Speed (miles per hour)', 'Travel time', 'Segment distance (miles)',
                      'Route', 'Frequency (trips per hour)', 'Shape ID',
                      'Stop Sequence']
    tooltip_dict = {'aliases': display_aliases}

    title = f"{district} Better Buses Speeds, Corridors, and Hotspots"
    colorscale.caption = "Speed (miles per hour)"
    style_dict = {'opacity': 0, 'fillOpacity': 0.8}

    g = gdf.explore(column='_20p_mph',
                    cmap = colorscale,
                    tiles = 'CartoDB positron',
                    style_kwds = style_dict,
                    tooltip = display_cols, popup = display_cols,
                    tooltip_kwds = tooltip_dict, popup_kwds = tooltip_dict,
                    highlight_kwds = {'fillColor': '#DD1C77',"fillOpacity": 0.6},
                    width = size[0], height = size[1], zoom_start = 13,
                    location = centroid,
                   m = m)

    title_html = f"""
     <h3 align="center" style="font-size:20px"><b>{title}</b></h3>
     """
    g.get_root().html.add_child(folium.Element(title_html)) # might still want a util for this...
    return g
    
def bb_map_all(hotspots, corridors, combined_speeds, district):
    '''
    Renders a unified map of hotspots, corridors, and segment speeds to visualize
    better buses reccomendations.
    hotspots: list of hotspot corridor gdfs (RtFilterMapper.corridor)
    corridors: list of corridor corridor gdfs (RtFilterMapper.corridor)
    combined_speeds: combined RtFilterMapper.detailed_map_view(s)
    '''
    
    display_cols = ['schedule_metric_minutes', 'speed_metric_minutes',
                   'routes_included', 'agency']
    display_aliases = ['Schedule-Based Delay Metric (minutes)',
                      'Speed-Based Delay Metric (minutes)',
                      'Routes Included', 'Agency']
    tooltip_dict = {'aliases': display_aliases}
    style_dict = {'opacity': 0, 'fillOpacity': 0.8}
    
    if corridors:
        all_corr = pd.concat(corridors)
        all_corr['location_type'] = 'corridor'
        combined = all_corr
    if hotspots:
        all_hs = pd.concat(hotspots)
        all_hs['location_type'] = 'hotspot'
        combined = all_hs
    if corridors and hotspots:
        combined = pd.concat([all_corr, all_hs])
    cmap = ['#1b9e77', '#d95f02']
    
    m = combined.explore(tiles = 'CartoDB positron',
                                    column = 'location_type', cmap = cmap,
                                    tooltip = display_cols, popup = display_cols,
                                    tooltip_kwds = tooltip_dict, popup_kwds = tooltip_dict,
                                   )
    m = speedmap_from_combined(combined_speeds, district, m = m)
    return m