import datetime as dt

import geopandas as gpd
import matplotlib.pyplot as plt
import pandas as pd

# import rt_utils
import seaborn as sns
import shapely

# import shared_utils
from IPython.display import Markdown, display
from shared_utils import calitp_color_palette as cp
from shared_utils import geography_utils, map_utils, rt_utils, utils
from siuba import *

# from tqdm import tqdm

GCS_FILE_PATH = rt_utils.GCS_FILE_PATH


class RtFilterMapper:
    """
    Collects filtering and mapping functions, init with stop segment speed view and rt_trips
    """

    def __init__(self, rt_trips, stop_delay_view, routelines, pbar=None):
        self.pbar = pbar
        self.rt_trips = rt_trips
        self.calitp_itp_id = self.rt_trips.calitp_itp_id.iloc[0]
        self.stop_delay_view = stop_delay_view
        self.routelines = routelines
        self.calitp_agency_name = rt_trips.calitp_agency_name.iloc[0]
        self.analysis_date = rt_trips.service_date.iloc[0]
        self.display_date = self.analysis_date.strftime(rt_utils.DATE_WEEKDAY_FMT)

        self.endpoint_delay_view = (
            self.stop_delay_view
            >> group_by(_.trip_id)
            >> filter(_.stop_sequence == _.stop_sequence.max())
            >> ungroup()
            >> mutate(arrival_hour=_.arrival_time.apply(lambda x: x.hour))
            >> inner_join(
                _, self.rt_trips >> select(_.trip_id, _.mean_speed_mph), on="trip_id"
            )
        )
        self.endpoint_delay_summary = (
            self.endpoint_delay_view
            >> group_by(_.direction_id, _.route_id, _.arrival_hour)
            >> summarize(
                n_trips=_.route_id.size, mean_end_delay_seconds=_.delay_seconds.mean()
            )
        )
        self.reset_filter()

    def set_filter(
        self,
        start_time=None,
        end_time=None,
        route_names=None,
        shape_ids=None,
        direction_id=None,
        direction=None,
    ):
        """
        start_time, end_time: string %H:%M, for example '11:00' and '14:00'
        route_names: list or pd.Series of route_names (GTFS route_short_name)
        direction_id: '0' or '1'
        direction: string 'Northbound', 'Eastbound', 'Southbound', 'Westbound' (experimental)
        """
        assert (
            start_time
            or end_time
            or route_names
            or direction_id
            or direction
            or shape_ids
        ), "must supply at least 1 argument to filter"
        assert not start_time or isinstance(
            dt.datetime.strptime(start_time, rt_utils.HOUR_MIN_FMT), dt.datetime
        ), "invalid time string"
        assert not end_time or isinstance(
            dt.datetime.strptime(end_time, rt_utils.HOUR_MIN_FMT), dt.datetime
        ), "invalid time string"
        assert not route_names or isinstance(route_names, (list, tuple, pd.Series))
        if route_names:
            # print(route_names)
            # print(type(route_names))
            assert (
                pd.Series(route_names).isin(self.rt_trips.route_short_name).all()
            ), "at least 1 route not found in self.rt_trips"
        assert (
            not direction_id or isinstance(direction_id, str) and len(direction_id) == 1
        )
        self.filter = {}
        if start_time:
            self.filter["start_time"] = dt.datetime.strptime(
                start_time, rt_utils.HOUR_MIN_FMT
            ).time()
        else:
            self.filter["start_time"] = None
        if end_time:
            self.filter["end_time"] = dt.datetime.strptime(
                end_time, rt_utils.HOUR_MIN_FMT
            ).time()
        else:
            self.filter["end_time"] = None
        self.filter["route_names"] = route_names
        self.filter["shape_ids"] = shape_ids
        if shape_ids:
            shape_trips = (
                self.rt_trips
                >> filter(_.shape_id.isin(shape_ids))
                >> distinct(_.route_short_name, _keep_all=True)
                >> collect()
            )
            self.filter["route_names"] = list(shape_trips.route_short_name)
            if len(shape_ids) == 1:
                direction = (
                    self.rt_trips >> filter(_.shape_id == shape_ids[0])
                ).direction.iloc[0]
        self.filter["direction_id"] = direction_id
        self.filter["direction"] = direction
        if start_time and end_time:
            self.hr_duration_in_filter = (
                dt.datetime.combine(self.analysis_date, self.filter["end_time"])
                - dt.datetime.combine(self.analysis_date, self.filter["start_time"])
            ).seconds / 60**2
        else:
            self.hr_duration_in_filter = (
                self.stop_delay_view.actual_time.max()
                - self.stop_delay_view.actual_time.min()
            ).seconds / 60**2
        if self.filter["route_names"] and len(self.filter["route_names"]) < 5:
            rts = "Route(s) " + ", ".join(self.filter["route_names"])
        elif self.filter["route_names"] and len(self.filter["route_names"]) > 5:
            rts = "Multiple Routes"
        elif not self.filter["route_names"]:
            rts = "All Routes"

        # print(self.filter)
        # properly format for pm peak, TODO add other periods
        if start_time and end_time and start_time == "15:00" and end_time == "19:00":
            self.filter_period = "PM_Peak"
        elif start_time and end_time and start_time == "06:00" and end_time == "09:00":
            self.filter_period = "AM_Peak"
        elif start_time and end_time and start_time == "10:00" and end_time == "14:00":
            self.filter_period = "Midday"
        elif not start_time and not end_time:
            self.filter_period = "All_Day"
        else:
            self.filter_period = f"{start_time}–{end_time}"
        elements_ordered = [rts, direction, self.filter_period, self.display_date]
        self.filter_formatted = ", " + ", ".join(
            [str(x).replace("_", " ") for x in elements_ordered if x]
        )

        self._time_only_filter = (
            not route_names and not shape_ids and not direction_id and not direction
        )

    def reset_filter(self):
        self.filter = None
        self.filter_period = "All_Day"
        rts = "All Routes"
        elements_ordered = [rts, self.filter_period, self.display_date]
        self.filter_formatted = ", " + ", ".join(
            [str(x).replace("_", " ") for x in elements_ordered if x]
        )

    def _filter(self, df):
        """Filters a df (containing trip_id) on trip_id based on any set filter."""
        if self.filter is None:
            return df
        else:
            trips = self.rt_trips.copy()
            # print(f'view filter: {self.filter}')
        if self.filter["start_time"]:
            trips = trips >> filter(_.median_time > self.filter["start_time"])
        if self.filter["end_time"]:
            trips = trips >> filter(_.median_time < self.filter["end_time"])
        if self.filter["route_names"]:
            trips = trips >> filter(_.route_short_name.isin(self.filter["route_names"]))
        if self.filter["shape_ids"]:
            trips = trips >> filter(_.shape_id.isin(self.filter["shape_ids"]))
        if self.filter["direction_id"]:
            trips = trips >> filter(_.direction_id == self.filter["direction_id"])
        if self.filter["direction"]:
            trips = trips >> filter(_.direction == self.filter["direction"])
        return df.copy() >> inner_join(_, trips >> select(_.trip_id), on="trip_id")

    # TODO, optionally port over segment speed map from rt_analysis, would require stop delay view
    # alternatively, simply export full day segment speed view

    def segment_speed_map(
        self,
        segments="stops",
        how="average",
        colorscale=rt_utils.ZERO_THIRTY_COLORSCALE,
        size=[900, 550],
        no_title=False,
    ):
        """Generate a map of segment speeds aggregated across all trips for each shape,
        either as averages
        or 20th percentile speeds.

        segments: 'stops' or 'detailed' (detailed not yet implemented)
        how: 'average' or 'low_speeds'
        colorscale: branca.colormap
        size: [x, y]

        """
        assert segments in ["stops", "detailed"]
        assert how in ["average", "low_speeds"]

        gcs_filename = (
            f"{self.calitp_itp_id}_{self.analysis_date.strftime(rt_utils.MONTH_DAY_FMT)}"
            f"_{self.filter_period}"
        )
        subfolder = "segment_speed_views/"
        cached_periods = ["PM_Peak", "AM_Peak", "Midday", "All_Day"]
        if (
            rt_utils.check_cached(f"{gcs_filename}.parquet", subfolder)
            and self.filter_period in cached_periods
            and self._time_only_filter
        ):
            self.stop_segment_speed_view = gpd.read_parquet(
                f"{GCS_FILE_PATH}{subfolder}{gcs_filename}.parquet"
            )
        else:
            gdf = self._filter(self.stop_delay_view)
            all_stop_speeds = gpd.GeoDataFrame()
            # for shape_id in tqdm(gdf.shape_id.unique()): trying self.pbar.update...
            if not isinstance(self.pbar, type(None)):
                self.pbar.reset(total=len(gdf.shape_id.unique()))
                self.pbar.desc = "Generating segment speeds"
            for shape_id in gdf.shape_id.unique():
                this_shape = (gdf >> filter((_.shape_id == shape_id))).copy()
                if this_shape.empty:
                    # print(f'{shape_id} empty!')
                    continue
                # self.debug_dict[f'{shape_id}_{direction_id}_tsd'] = this_shape_direction
                stop_speeds = (
                    this_shape
                    >> group_by(_.trip_key)
                    >> arrange(_.stop_sequence)
                    >> mutate(
                        seconds_from_last=(
                            _.actual_time - _.actual_time.shift(1)
                        ).apply(lambda x: x.seconds)
                    )
                    >> mutate(last_loc=_.shape_meters.shift(1))
                    >> mutate(meters_from_last=(_.shape_meters - _.last_loc))
                    >> mutate(speed_from_last=_.meters_from_last / _.seconds_from_last)
                    # >> mutate(last_delay = _.delay.shift(1))
                    >> mutate(
                        delay_chg_sec=(_.delay_seconds - _.delay_seconds.shift(1))
                    )
                    # >> mutate(delay_sec = _.delay.map(lambda x: x.seconds if x.days == 0 else x.seconds - 24*60**2))
                    >> ungroup()
                )
                if stop_speeds.empty:
                    # print(f'{shape_id}_{direction_id}_st_spd empty!')
                    continue
                # self.debug_dict[f'{shape_id}_{direction_id}_st_spd'] = stop_speeds
                stop_speeds.geometry = stop_speeds.apply(
                    lambda x: shapely.ops.substring(
                        (
                            self.routelines >> filter(_.shape_id == x.shape_id)
                        ).geometry.iloc[0],
                        x.last_loc,
                        x.shape_meters,
                    ),
                    axis=1,
                )
                stop_speeds = stop_speeds.dropna(subset=["last_loc"]).set_crs(
                    geography_utils.CA_NAD83Albers
                )

                try:
                    stop_speeds = (
                        stop_speeds
                        >> mutate(speed_mph=_.speed_from_last * rt_utils.MPH_PER_MPS)
                        >> group_by(_.stop_sequence)
                        >> mutate(
                            n_trips=_.stop_sequence.size,
                            avg_mph=_.speed_mph.mean(),
                            _20p_mph=_.speed_mph.quantile(0.2),
                            trips_per_hour=_.n_trips / self.hr_duration_in_filter,
                        )
                        # >> distinct(_.stop_sequence, _keep_all=True) ## comment out to enable speed distribution analysis
                        >> ungroup()
                        >> select(
                            -_.arrival_time, -_.actual_time, -_.delay, -_.last_delay
                        )
                    )
                    # self.debug_dict[f'{shape_id}_{direction_id}_st_spd2'] = stop_speeds
                    assert not stop_speeds.empty, "stop speeds gdf is empty!"
                except Exception:  # as e:
                    # print(f'stop_speeds shape: {stop_speeds.shape}, shape_id: {shape_id}')
                    # print(e)
                    continue
                # Drop impossibly high speeds
                stop_speeds = stop_speeds >> filter(_.speed_mph < 80)
                if stop_speeds.avg_mph.max() > 80:
                    # print(f'speed above 80 for shape {stop_speeds.shape_id.iloc[0]}, dropping')
                    stop_speeds = stop_speeds >> filter(_.avg_mph < 80)
                if stop_speeds._20p_mph.min() < 0:
                    # print(f'negative speed for shape {stop_speeds.shape_id.iloc[0]}, dropping')
                    stop_speeds = stop_speeds >> filter(_._20p_mph > 0)
                all_stop_speeds = pd.concat((all_stop_speeds, stop_speeds))

                if not isinstance(self.pbar, type(None)):
                    self.pbar.update()
                if not isinstance(self.pbar, type(None)):
                    self.pbar.refresh
            if self._time_only_filter:
                all_stop_speeds = all_stop_speeds >> select(
                    -_.speed_mph, -_.speed_from_last, -_.trip_id, -_.trip_key
                )
            self.stop_segment_speed_view = all_stop_speeds
            export_path = f"{GCS_FILE_PATH}segment_speed_views/"
            if self._time_only_filter:
                utils.geoparquet_gcs_export(all_stop_speeds, export_path, gcs_filename)
        return self._show_speed_map(
            how=how, colorscale=colorscale, size=size, no_title=no_title
        )

    def _show_speed_map(
        self,
        how="average",
        colorscale=rt_utils.ZERO_THIRTY_COLORSCALE,
        size=[900, 550],
        no_title=False,
    ):

        gdf = self.stop_segment_speed_view.copy()
        # gdf = self._filter(gdf)
        # print(gdf.dtypes)
        # singletrip = gdf.trip_id.nunique() == 1 ## incompatible with current caching approach
        gdf = gdf >> distinct(
            _.shape_id, _.stop_sequence, _keep_all=True
        )  # essential here for reasonable map size!
        orig_rows = gdf.shape[0]
        gdf["shape_miles"] = gdf.shape_meters / 1609
        gdf = gdf.round(
            {"avg_mph": 1, "_20p_mph": 1, "shape_miles": 1, "trips_per_hour": 1}
        )  # round for display

        how_speed_col = {"average": "avg_mph", "low_speeds": "_20p_mph"}
        how_formatted = {"average": "Average", "low_speeds": "20th Percentile"}

        gdf = (
            gdf
            >> select(
                -_.service_date,
                -_.last_loc,
                -_.shape_meters,
                -_.meters_from_last,
                -_.n_trips,
            )  # drop unused cols for smaller map size
            >> arrange(_.trips_per_hour)
        ).set_crs(geography_utils.CA_NAD83Albers)

        # shift to right side of road to display direction
        gdf.geometry = gdf.geometry.apply(rt_utils.try_parallel)
        self.detailed_map_view = gdf.copy()
        # create clips, integrate buffer+simplify?
        gdf.geometry = gdf.geometry.apply(rt_utils.arrowize_segment).simplify(
            tolerance=5
        )
        gdf = gdf >> filter(gdf.geometry.is_valid) >> filter(-gdf.geometry.is_empty)

        assert (
            gdf.shape[0] >= orig_rows * 0.99
        ), "over 1% of geometries invalid after buffer+simplify"
        gdf = gdf.to_crs(geography_utils.WGS84)
        centroid = (gdf.geometry.centroid.x.mean(), gdf.geometry.centroid.y.mean())
        # centroid = gdf.dissolve().centroid
        name = self.calitp_agency_name

        popup_dict = {
            how_speed_col[how]: "Speed (miles per hour)",
            "route_short_name": "Route",
            # "shape_id": "Shape ID",
            # "direction_id": "Direction ID",
            # "stop_id": "Next Stop ID",
            # "stop_sequence": "Next Stop Sequence",
            "trips_per_hour": "Frequency (trips per hour)",
            "shape_miles": "Distance from start of route (miles)",
        }
        # if singletrip:
        #     popup_dict["delay_seconds"] = "Current Delay (seconds)"
        #     popup_dict["delay_chg_sec"] = "Change in Delay (seconds)"
        if no_title:
            title = ""
        else:
            title = f"{name} {how_formatted[how]} Vehicle Speeds Between Stops{self.filter_formatted}"

        g = map_utils.make_folium_choropleth_map(
            gdf,
            plot_col=how_speed_col[how],
            popup_dict=popup_dict,
            tooltip_dict=popup_dict,
            colorscale=colorscale,
            fig_width=size[0],
            fig_height=size[1],
            zoom=13,
            centroid=[centroid[1], centroid[0]],
            title=title,
            legend_name="Speed (miles per hour)",
            highlight_function=lambda x: {
                "fillColor": "#DD1C77",
                "fillOpacity": 0.6,
            },
            reduce_precision=False,
        )

        return g

    def chart_delays(self, no_title=False):
        """
        A bar chart showing delays grouped by arrival hour for current filtered selection.
        Currently hardcoded to 0600-2200.
        """
        filtered_endpoint = (
            self._filter(self.endpoint_delay_view)
            >> filter(_.arrival_hour > 5)
            >> filter(_.arrival_hour < 23)
        )
        grouped = (
            filtered_endpoint
            >> group_by(_.arrival_hour)
            >> summarize(mean_end_delay=_.delay_seconds.mean())
        )
        grouped["Minutes of Delay at Endpoint"] = grouped.mean_end_delay.apply(
            lambda x: x / 60
        )
        grouped["Hour"] = grouped.arrival_hour
        if no_title:
            title = ""
        else:
            title = f"{self.calitp_agency_name} Mean Delays by Arrival Hour{self.filter_formatted}"

        sns_plot = sns.barplot(
            x=grouped["Hour"],
            y=grouped["Minutes of Delay at Endpoint"],
            ci=None,
            palette=[cp.CALITP_CATEGORY_BOLD_COLORS[1]],
        ).set_title(title)
        chart = sns_plot.get_figure()
        chart.tight_layout()
        return chart

    def chart_variability(
        self, min_stop_seq=None, max_stop_seq=None, num_segments=None, no_title=False
    ):
        """
        Chart trip speed variability, as speed between each stop segments.
        stop_sequence_range: (min_stop, max_stop)
        """
        sns.set(rc={"figure.figsize": (13, 6)})
        assert (
            self.filter["shape_ids"] and len(self.filter["shape_ids"]) == 1
        ), "must filter to a single shape_id"
        # _map = self.segment_speed_map()
        to_chart = self.stop_segment_speed_view.copy()
        if num_segments:
            unique_stops = list(self.stop_segment_speed_view.stop_sequence.unique())[
                :num_segments
            ]
            min_stop_seq = min(unique_stops)
            max_stop_seq = max(unique_stops)
        if min_stop_seq:
            to_chart = to_chart >> filter(_.stop_sequence >= min_stop_seq)
        if max_stop_seq:
            to_chart = to_chart >> filter(_.stop_sequence <= max_stop_seq)
        to_chart.stop_name = to_chart.stop_name.str.split("&").map(lambda x: x[-1])
        to_chart = to_chart.rename(
            columns={
                "speed_mph": "Segement Speed (mph)",
                "delay_chg_sec": "Increase in Delay (seconds)",
                "stop_sequence": "Stop Segment ID",
                "stop_name": "Segment Cross Street",
            }
        )
        plt.xticks(rotation=65)
        title = f"{self.calitp_agency_name} Speed Variability by Stop Segment{self.filter_formatted}"
        if no_title:
            title = None
        variability_plt = sns.swarmplot(
            x=to_chart["Segment Cross Street"],
            y=to_chart["Segement Speed (mph)"],
            palette=cp.CALITP_CATEGORY_BRIGHT_COLORS,
        ).set_title(title)
        return variability_plt

    # works fine, not especially handy to agencies
    # def describe_delayed_routes(self):
    #     try:
    #         most_delayed = (self._filter(self.endpoint_delay_view)
    #                     >> group_by(_.shape_id)
    #                     >> summarize(mean_delay_seconds = _.delay_seconds.mean())
    #                     >> arrange(-_.mean_delay_seconds)
    #                     >> head(5)
    #                    )
    #         most_delayed = most_delayed >> inner_join(_,
    #                                self.rt_trips >> distinct(_.shape_id, _keep_all=True),
    #                                on = 'shape_id')
    #         most_delayed = most_delayed.apply(rt_utils.describe_most_delayed, axis=1)
    #         display_list = most_delayed.full_description.to_list()
    #         with_newlines = '\n * ' + '\n * '.join(display_list)
    #         period_formatted = self.filter_period.replace('_', ' ')
    #         display(Markdown(f'{period_formatted} most delayed routes: {with_newlines}'))
    #     except:
    #         # print('describe delayed routes failed!')
    #         pass
    #     return

    def describe_slow_routes(self):
        try:
            slowest = (
                self._filter(self.rt_trips)
                >> group_by(_.shape_id)
                >> summarize(
                    num_trips=_.shape_id.count(),
                    median_trip_mph=_.mean_speed_mph.median(),
                )
                >> arrange(_.median_trip_mph)
                >> inner_join(
                    _,
                    self._filter(self.rt_trips)
                    >> distinct(
                        _.shape_id,
                        _.route_id,
                        _.route_short_name,
                        _.route_long_name,
                        _.route_desc,
                        _.direction,
                    ),
                    on="shape_id",
                )
                >> head(7)
            )
            slowest = slowest.apply(rt_utils.describe_slowest, axis=1)
            display_list = slowest.full_description.to_list()
            with_newlines = "\n * " + "\n * ".join(display_list)
            period_formatted = self.filter_period.replace("_", " ")
            # display(slowest)
            display(Markdown(f"{period_formatted} slowest routes: {with_newlines}"))
        except Exception:
            # print('describe slow routes failed!')
            pass
        return


def from_gcs(itp_id, analysis_date):
    """Generates RtFilterMapper from cached artifacts in GCS.
    Generate using rt_analysis.OperatorDayAnalysis.export_views_gcs()"""

    month_day = analysis_date.strftime(rt_utils.MONTH_DAY_FMT)
    trips = pd.read_parquet(f"{GCS_FILE_PATH}rt_trips/{itp_id}_{month_day}.parquet")
    stop_delay = gpd.read_parquet(
        f"{GCS_FILE_PATH}stop_delay_views/{itp_id}_{month_day}.parquet"
    )
    stop_delay["arrival_time"] = stop_delay.arrival_time.map(
        lambda x: dt.datetime.fromisoformat(x)
    )
    stop_delay["actual_time"] = stop_delay.actual_time.map(
        lambda x: dt.datetime.fromisoformat(x)
    )
    routelines = rt_utils.get_routelines(itp_id, analysis_date)
    rt_day = RtFilterMapper(trips, stop_delay, routelines)
    return rt_day
