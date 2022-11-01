import numpy as np
import datetime as dt
import geopandas as gpd
from rt_analysis import rt_filter_map_plot

def sccp_average_metrics(itp_id: int, date_range: np.arange, corridor: gpd.GeoDataFrame,
                        filter_dict: dict = None):
    '''
    Generate averaged corridor delay metrics for SCCP, LPP programs. For 2022-2023 cycle,
    default range is Apr 30 2022 to May 8 2022:
    date_range = np.arange('2022-04-30', '2022-05-09', dtype='datetime64[D]')
    
    Optional filter_dict should _not_ be used to generate SCCP/LPP metrics, but is available
    for exploratory analysis and other analyses. Supply RtFilterMapper.set_filter params
    as dict keys with each param's value as dict values:
    filter_dict = {'start_time': '06:00', 'end_time': '10:00', 'route_names': ['2', 'R12']}
    '''
    schedule_metrics = []
    speed_metrics = []
    for date in date_range:
        date = date.astype(dt.date)
        try:
            rt_day = rt_filter_map_plot.from_gcs(itp_id, date)
            rt_day.add_corridor(corridor)
            if filter_dict:
                rt_day.set_filter(**filter_dict)
            metrics = rt_day.corridor_metrics()
            schedule_metrics += [metrics['schedule_metric_minutes']]
            speed_metrics += [metrics['speed_metric_minutes']]
            print(f'complete for date: {date}')
        except Exception as e:
            print(f'failed for date: {date}')
            print(e)
            continue
    schedule_metric = np.round(np.mean(schedule_metrics), 0)
    speed_metric = np.round(np.mean(speed_metrics), 0)
    return {'avg_schedule_metric_minutes': schedule_metric,
           'avg_speed_metric_minutes': speed_metric,
           'all_schedule': schedule_metrics,
           'all_speed': speed_metrics}