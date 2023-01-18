import pandas as pd
import geopandas as gpd
from siuba import *
import datetime as dt

from bus_service_utils import better_bus_utils
from rt_analysis import rt_filter_map_plot, rt_parser


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
            fm = rt_filter_map_plot.from_gcs(itp_id=itp_id, analysis_date=analysis_date)
        rt_id_names = (fm.rt_trips
                 >> distinct(_.route_id, _.route_short_name)
                 >> inner_join(_, bbutil_gdf >> select(_.route_id), on = 'route_id')
                )
        fm.set_filter(route_names = rt_id_names.route_short_name)
        fm_dict_new[itp_id] = fm
    print(fm_dict_new)
    return fm_dict_new