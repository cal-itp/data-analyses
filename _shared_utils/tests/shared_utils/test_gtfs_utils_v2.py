import datetime
import re

import geopandas as gpd
import pandas as pd
import pytest
import sqlalchemy
from calitp_data_analysis import geography_utils
from dateutil.relativedelta import relativedelta
from pandas._libs.tslibs.timestamps import Timestamp
from pytest_unordered import unordered
from shapely import LineString
from shapely.geometry import Point
from shared_utils.gtfs_utils_v2 import (
    filter_to_public_schedule_gtfs_dataset_keys,
    get_metrolink_feed_key,
    get_shapes,
    get_stop_times,
    get_stops,
    get_trips,
    schedule_daily_feed_to_gtfs_dataset_name,
)


class TestGtfsUtilsV2:
    @pytest.fixture
    def trip(self):
        return pd.DataFrame(
            data=[
                {
                    "feed_key": "4321a7e3901b2275805494a746ec1c6a",
                    "trip_id": "(MWF)_t_5405971_b_55349_tn_0",
                }
            ]
        )

    @pytest.mark.vcr
    def test_get_metrolink_feed_key(self):
        result = get_metrolink_feed_key(selected_date="2025-08-23")

        assert result == "0b0ebeff0c1f7ff681e6a06d6218ecd6"

    @pytest.mark.default_cassette("TestGtfsUtilsV2.test_get_metrolink_feed_key.yaml")
    @pytest.mark.vcr
    def test_get_metrolink_feed_key_get_df(self):
        result = get_metrolink_feed_key(selected_date="2025-08-23", get_df=True)

        assert len(result) == 1
        assert result.to_dict(orient="records") == [
            {"feed_key": "0b0ebeff0c1f7ff681e6a06d6218ecd6", "name": "Metrolink Schedule"}
        ]

    @pytest.mark.vcr
    def test_schedule_daily_feed_to_gtfs_dataset_name(self):
        result = schedule_daily_feed_to_gtfs_dataset_name(selected_date="2025-09-01")

        assert len(result) == 2
        assert result.to_dict(orient="records") == unordered(
            [
                {
                    "key": "80a64851bc2bcae60ebb5f8a56148ad9",
                    "date": Timestamp("2025-09-01 00:00:00"),
                    "feed_key": "3bbe132a97a1510d3e1a265875ef7590",
                    "feed_timezone": None,
                    "base64_url": "aHR0cHM6Ly9hcGkuNTExLm9yZy90cmFuc2l0L2RhdGFmZWVkcz9vcGVyYXRvcl9pZD1DRQ==",
                    "gtfs_dataset_key": "9f2566d34fde8d7d0d51b64bdf77a7ba",
                    "name": "Bay Area 511 ACE Schedule",
                    "type": "schedule",
                    "regional_feed_type": "Regional Subfeed",
                },
                {
                    "key": "625d9c588e8b936220a06bb85a7c063d",
                    "date": Timestamp("2025-09-01 00:00:00"),
                    "feed_key": "0874d9772a918edbedff0493590d626a",
                    "feed_timezone": "America/New_York",
                    "base64_url": "aHR0cHM6Ly9jb250ZW50LmFtdHJhay5jb20vY29udGVudC9ndGZzL0dURlMuemlw",
                    "gtfs_dataset_key": "1165b1474df778cb0fc3ba9246e32035",
                    "name": "Amtrak Schedule",
                    "type": "schedule",
                    "regional_feed_type": None,
                },
            ]
        )

    @pytest.mark.default_cassette("TestGtfsUtilsV2.test_schedule_daily_feed_to_gtfs_dataset_name.yaml")
    @pytest.mark.vcr
    def test_schedule_daily_feed_to_gtfs_dataset_name_get_df_false(self):
        result = schedule_daily_feed_to_gtfs_dataset_name(selected_date="2025-09-01", get_df=False)

        assert isinstance(result, sqlalchemy.sql.selectable.Select)

    @pytest.mark.vcr
    def test_schedule_daily_feed_to_gtfs_dataset_name_keep_cols(self):
        result = schedule_daily_feed_to_gtfs_dataset_name(
            selected_date="2025-09-01",
            keep_cols=["name", "gtfs_dataset_key", "feed_key"],
        )

        assert len(result) == 2
        assert result.to_dict(orient="records") == unordered(
            [
                {
                    "name": "Bay Area 511 ACE Schedule",
                    "gtfs_dataset_key": "9f2566d34fde8d7d0d51b64bdf77a7ba",
                    "feed_key": "3bbe132a97a1510d3e1a265875ef7590",
                },
                {
                    "name": "Amtrak Schedule",
                    "gtfs_dataset_key": "1165b1474df778cb0fc3ba9246e32035",
                    "feed_key": "0874d9772a918edbedff0493590d626a",
                },
            ]
        )

    @pytest.mark.vcr
    def test_schedule_daily_feed_to_gtfs_dataset_name_feed_option_customer_facing(self):
        result = schedule_daily_feed_to_gtfs_dataset_name(
            selected_date="2025-09-01",
            feed_option="customer_facing",
        )

        assert len(result) == 2
        assert result.to_dict(orient="records") == unordered(
            [
                {
                    "key": "361663508967ce62a3557993829f8bf8",
                    "date": Timestamp("2025-09-01 00:00:00"),
                    "feed_key": "c86f88ad0b15f5185d073f91f2130285",
                    "feed_timezone": "America/Los_Angeles",
                    "base64_url": "aHR0cHM6Ly9hcGkuNTExLm9yZy90cmFuc2l0L2RhdGFmZWVkcz9vcGVyYXRvcl9pZD1SRw==",
                    "gtfs_dataset_key": "d6ed100168196d507b4ef1c0d111ee72",
                    "name": "Bay Area 511 Regional Schedule",
                    "regional_feed_type": "Combined Regional Feed",
                    "type": "schedule",
                },
                {
                    "key": "625d9c588e8b936220a06bb85a7c063d",
                    "date": Timestamp("2025-09-01 00:00:00"),
                    "feed_key": "0874d9772a918edbedff0493590d626a",
                    "feed_timezone": "America/New_York",
                    "base64_url": "aHR0cHM6Ly9jb250ZW50LmFtdHJhay5jb20vY29udGVudC9ndGZzL0dURlMuemlw",
                    "gtfs_dataset_key": "1165b1474df778cb0fc3ba9246e32035",
                    "name": "Amtrak Schedule",
                    "regional_feed_type": None,
                    "type": "schedule",
                },
            ]
        )

    @pytest.mark.vcr
    def test_schedule_daily_feed_to_gtfs_dataset_name_feed_option_current_feeds(self):
        result = schedule_daily_feed_to_gtfs_dataset_name(
            selected_date="2025-09-01",
            feed_option="current_feeds",
        )

        assert len(result) == 3
        assert result.to_dict(orient="records") == unordered(
            [
                {
                    "key": "80a64851bc2bcae60ebb5f8a56148ad9",
                    "date": Timestamp("2025-09-01 00:00:00"),
                    "feed_key": "3bbe132a97a1510d3e1a265875ef7590",
                    "feed_timezone": None,
                    "base64_url": "aHR0cHM6Ly9hcGkuNTExLm9yZy90cmFuc2l0L2RhdGFmZWVkcz9vcGVyYXRvcl9pZD1DRQ==",
                    "gtfs_dataset_key": "9f2566d34fde8d7d0d51b64bdf77a7ba",
                    "name": "Bay Area 511 ACE Schedule",
                    "regional_feed_type": "Regional Subfeed",
                    "type": "schedule",
                },
                {
                    "key": "361663508967ce62a3557993829f8bf8",
                    "date": Timestamp("2025-09-01 00:00:00"),
                    "feed_key": "c86f88ad0b15f5185d073f91f2130285",
                    "feed_timezone": "America/Los_Angeles",
                    "base64_url": "aHR0cHM6Ly9hcGkuNTExLm9yZy90cmFuc2l0L2RhdGFmZWVkcz9vcGVyYXRvcl9pZD1SRw==",
                    "gtfs_dataset_key": "d6ed100168196d507b4ef1c0d111ee72",
                    "name": "Bay Area 511 Regional Schedule",
                    "regional_feed_type": "Combined Regional Feed",
                    "type": "schedule",
                },
                {
                    "key": "625d9c588e8b936220a06bb85a7c063d",
                    "date": Timestamp("2025-09-01 00:00:00"),
                    "feed_key": "0874d9772a918edbedff0493590d626a",
                    "feed_timezone": "America/New_York",
                    "base64_url": "aHR0cHM6Ly9jb250ZW50LmFtdHJhay5jb20vY29udGVudC9ndGZzL0dURlMuemlw",
                    "gtfs_dataset_key": "1165b1474df778cb0fc3ba9246e32035",
                    "name": "Amtrak Schedule",
                    "regional_feed_type": None,
                    "type": "schedule",
                },
            ]
        )

    @pytest.mark.vcr
    def test_schedule_daily_feed_to_gtfs_dataset_name_feed_option_include_precursor(self):
        result = schedule_daily_feed_to_gtfs_dataset_name(
            selected_date="2025-09-01",
            feed_option="include_precursor",
        )

        assert len(result) == 4
        assert result.to_dict(orient="records") == unordered(
            [
                {
                    "key": "80a64851bc2bcae60ebb5f8a56148ad9",
                    "date": Timestamp("2025-09-01 00:00:00"),
                    "feed_key": "3bbe132a97a1510d3e1a265875ef7590",
                    "feed_timezone": None,
                    "base64_url": "aHR0cHM6Ly9hcGkuNTExLm9yZy90cmFuc2l0L2RhdGFmZWVkcz9vcGVyYXRvcl9pZD1DRQ==",
                    "gtfs_dataset_key": "9f2566d34fde8d7d0d51b64bdf77a7ba",
                    "name": "Bay Area 511 ACE Schedule",
                    "regional_feed_type": "Regional Subfeed",
                    "type": "schedule",
                },
                {
                    "key": "361663508967ce62a3557993829f8bf8",
                    "date": Timestamp("2025-09-01 00:00:00"),
                    "feed_key": "c86f88ad0b15f5185d073f91f2130285",
                    "feed_timezone": "America/Los_Angeles",
                    "base64_url": "aHR0cHM6Ly9hcGkuNTExLm9yZy90cmFuc2l0L2RhdGFmZWVkcz9vcGVyYXRvcl9pZD1SRw==",
                    "gtfs_dataset_key": "d6ed100168196d507b4ef1c0d111ee72",
                    "name": "Bay Area 511 Regional Schedule",
                    "regional_feed_type": "Combined Regional Feed",
                    "type": "schedule",
                },
                {
                    "key": "54749ffe7355490d3c2010e65aad95b8",
                    "date": Timestamp("2025-09-01 00:00:00"),
                    "feed_key": "11a1255a25c6e03696785b085a46f6a4",
                    "feed_timezone": "US/Pacific",
                    "base64_url": "aHR0cHM6Ly9hcGkuYWN0cmFuc2l0Lm9yZy90cmFuc2l0L2d0ZnMvZG93bmxvYWQ=",
                    "gtfs_dataset_key": "2f506f822a5f9b2afa48bda762a5e81d",
                    "name": "AC Transit Schedule",
                    "regional_feed_type": "Regional Precursor Feed",
                    "type": "schedule",
                },
                {
                    "key": "625d9c588e8b936220a06bb85a7c063d",
                    "date": Timestamp("2025-09-01 00:00:00"),
                    "feed_key": "0874d9772a918edbedff0493590d626a",
                    "feed_timezone": "America/New_York",
                    "base64_url": "aHR0cHM6Ly9jb250ZW50LmFtdHJhay5jb20vY29udGVudC9ndGZzL0dURlMuemlw",
                    "gtfs_dataset_key": "1165b1474df778cb0fc3ba9246e32035",
                    "name": "Amtrak Schedule",
                    "regional_feed_type": None,
                    "type": "schedule",
                },
            ]
        )

    @pytest.mark.vcr
    def test_get_trips(self):
        result = get_trips(selected_date="2025-09-01", operator_feeds=["c86f88ad0b15f5185d073f91f2130285"])

        assert len(result) == 3
        assert result.to_dict(orient="records") == unordered(
            [
                {
                    "key": "50e9e6e644f82ec29801c580ec0c772a",
                    "trip_instance_key": "d7d7502d292a35c41ee5a6c3c43f2fd5",
                    "feed_timezone": "America/Los_Angeles",
                    "base64_url": "aHR0cHM6Ly9hcGkuNTExLm9yZy90cmFuc2l0L2RhdGFmZWVkcz9vcGVyYXRvcl9pZD1SRw==",
                    "name": "Bay Area 511 Regional Schedule",
                    "regional_feed_type": "Combined Regional Feed",
                    "gtfs_dataset_key": "d6ed100168196d507b4ef1c0d111ee72",
                    "service_date": datetime.date(2025, 9, 1),
                    "feed_key": "c86f88ad0b15f5185d073f91f2130285",
                    "service_id": "3D:72971",
                    "trip_key": "7909be049636fabeaf2d7e519a099f23",
                    "trip_id": "3D:1358",
                    "iteration_num": 0,
                    "frequencies_defined_trip": False,
                    "trip_short_name": None,
                    "direction_id": 0,
                    "block_id": "3D:301",
                    "route_key": "bc23b5e936992cd318d7b35d3b18462b",
                    "route_id": "3D:374",
                    "route_type": "3",
                    "route_short_name": "374",
                    "route_long_name": "Pittsburg-Bay Point BART / Bay Point",
                    "route_continuous_pickup": None,
                    "route_continuous_drop_off": None,
                    "route_desc": None,
                    "route_color": "A03B00",
                    "route_text_color": "FFFFFF",
                    "agency_id": "3D",
                    "network_id": "3D",
                    "shape_array_key": "2ce3f738fbdcfdf229cab4c19b1dfc08",
                    "shape_id": "3D:25",
                    "contains_warning_duplicate_trip_primary_key": False,
                    "num_distinct_stops_served": 27,
                    "num_stop_times": 28,
                    "trip_first_departure_sec": 35460,
                    "trip_last_arrival_sec": 37560,
                    "trip_start_timezone": "America/Los_Angeles",
                    "trip_end_timezone": "America/Los_Angeles",
                    "service_hours": 0.5833333333333334,
                    "flex_service_hours": None,
                    "contains_warning_duplicate_stop_times_primary_key": False,
                    "contains_warning_missing_foreign_key_stop_id": False,
                    "trip_first_departure_ts": Timestamp("2025-09-01 16:51:00+0000", tz="UTC"),
                    "trip_last_arrival_ts": Timestamp("2025-09-01 17:26:00+0000", tz="UTC"),
                    "first_start_pickup_drop_off_window_sec": None,
                    "last_end_pickup_drop_off_window_sec": None,
                    "is_gtfs_flex_trip": False,
                    "is_entirely_demand_responsive_trip": False,
                    "num_gtfs_flex_stop_times": 0,
                    "num_approximate_timepoint_stop_times": 25,
                    "num_exact_timepoint_stop_times": 3,
                    "num_arrival_times_populated_stop_times": 28,
                    "num_departure_times_populated_stop_times": 28,
                    "trip_first_start_pickup_drop_off_window_ts": None,
                    "trip_last_end_pickup_drop_off_window_ts": None,
                    "trip_start_date_pacific": datetime.date(2025, 9, 1),
                    "trip_first_departure_datetime_pacific": Timestamp("2025-09-01 09:51:00"),
                    "trip_last_arrival_datetime_pacific": Timestamp("2025-09-01 10:26:00"),
                    "trip_start_date_local_tz": datetime.date(2025, 9, 1),
                    "trip_first_departure_datetime_local_tz": Timestamp("2025-09-01 09:51:00"),
                    "trip_last_arrival_datetime_local_tz": Timestamp("2025-09-01 10:26:00"),
                    "trip_first_start_pickup_drop_off_window_date_pacific": None,
                    "trip_first_start_pickup_drop_off_window_datetime_pacific": None,
                    "trip_last_end_pickup_drop_off_window_pacific": None,
                    "trip_first_start_pickup_drop_off_window_date_local_tz": None,
                    "trip_first_start_pickup_drop_off_window_datetime_local_tz": None,
                    "trip_last_end_pickup_drop_off_window_datetime_local_tz": None,
                    "time_of_day": "AM Peak",
                },
                {
                    "key": "791f8f41e0e017eecfe2fd8771e0c69b",
                    "trip_instance_key": "9299b8bc55f25684bfac1f9e3f08448f",
                    "feed_timezone": "America/Los_Angeles",
                    "base64_url": "aHR0cHM6Ly9hcGkuNTExLm9yZy90cmFuc2l0L2RhdGFmZWVkcz9vcGVyYXRvcl9pZD1SRw==",
                    "name": "Bay Area 511 Regional Schedule",
                    "regional_feed_type": "Combined Regional Feed",
                    "gtfs_dataset_key": "d6ed100168196d507b4ef1c0d111ee72",
                    "service_date": datetime.date(2025, 9, 1),
                    "feed_key": "c86f88ad0b15f5185d073f91f2130285",
                    "service_id": "3D:72971",
                    "trip_key": "2a1be21c6c902ddefe1bd13c7582c2c0",
                    "trip_id": "3D:2388",
                    "iteration_num": 0,
                    "frequencies_defined_trip": False,
                    "trip_short_name": None,
                    "direction_id": 0,
                    "block_id": "3D:310",
                    "route_key": "bc23b5e936992cd318d7b35d3b18462b",
                    "route_id": "3D:374",
                    "route_type": "3",
                    "route_short_name": "374",
                    "route_long_name": "Pittsburg-Bay Point BART / Bay Point",
                    "route_continuous_pickup": None,
                    "route_continuous_drop_off": None,
                    "route_desc": None,
                    "route_color": "A03B00",
                    "route_text_color": "FFFFFF",
                    "agency_id": "3D",
                    "network_id": "3D",
                    "shape_array_key": "2ce3f738fbdcfdf229cab4c19b1dfc08",
                    "shape_id": "3D:25",
                    "contains_warning_duplicate_trip_primary_key": False,
                    "num_distinct_stops_served": 27,
                    "num_stop_times": 28,
                    "trip_first_departure_sec": 42660,
                    "trip_last_arrival_sec": 44760,
                    "trip_start_timezone": "America/Los_Angeles",
                    "trip_end_timezone": "America/Los_Angeles",
                    "service_hours": 0.5833333333333334,
                    "flex_service_hours": None,
                    "contains_warning_duplicate_stop_times_primary_key": False,
                    "contains_warning_missing_foreign_key_stop_id": False,
                    "trip_first_departure_ts": Timestamp("2025-09-01 18:51:00+0000", tz="UTC"),
                    "trip_last_arrival_ts": Timestamp("2025-09-01 19:26:00+0000", tz="UTC"),
                    "first_start_pickup_drop_off_window_sec": None,
                    "last_end_pickup_drop_off_window_sec": None,
                    "is_gtfs_flex_trip": False,
                    "is_entirely_demand_responsive_trip": False,
                    "num_gtfs_flex_stop_times": 0,
                    "num_approximate_timepoint_stop_times": 25,
                    "num_exact_timepoint_stop_times": 3,
                    "num_arrival_times_populated_stop_times": 28,
                    "num_departure_times_populated_stop_times": 28,
                    "trip_first_start_pickup_drop_off_window_ts": None,
                    "trip_last_end_pickup_drop_off_window_ts": None,
                    "trip_start_date_pacific": datetime.date(2025, 9, 1),
                    "trip_first_departure_datetime_pacific": Timestamp("2025-09-01 11:51:00"),
                    "trip_last_arrival_datetime_pacific": Timestamp("2025-09-01 12:26:00"),
                    "trip_start_date_local_tz": datetime.date(2025, 9, 1),
                    "trip_first_departure_datetime_local_tz": Timestamp("2025-09-01 11:51:00"),
                    "trip_last_arrival_datetime_local_tz": Timestamp("2025-09-01 12:26:00"),
                    "trip_first_start_pickup_drop_off_window_date_pacific": None,
                    "trip_first_start_pickup_drop_off_window_datetime_pacific": None,
                    "trip_last_end_pickup_drop_off_window_pacific": None,
                    "trip_first_start_pickup_drop_off_window_date_local_tz": None,
                    "trip_first_start_pickup_drop_off_window_datetime_local_tz": None,
                    "trip_last_end_pickup_drop_off_window_datetime_local_tz": None,
                    "time_of_day": "Midday",
                },
                {
                    "key": "f789870e309d49e7611884ccbe992e01",
                    "trip_instance_key": "0a229cae7bc0a4d23e0a5163cc5b388d",
                    "feed_timezone": "America/Los_Angeles",
                    "base64_url": "aHR0cHM6Ly9hcGkuNTExLm9yZy90cmFuc2l0L2RhdGFmZWVkcz9vcGVyYXRvcl9pZD1SRw==",
                    "name": "Bay Area 511 Regional Schedule",
                    "regional_feed_type": "Combined Regional Feed",
                    "gtfs_dataset_key": "d6ed100168196d507b4ef1c0d111ee72",
                    "service_date": datetime.date(2025, 9, 1),
                    "feed_key": "c86f88ad0b15f5185d073f91f2130285",
                    "service_id": "3D:72971",
                    "trip_key": "89f044f6394d163eb0fb0af1e8d85bc4",
                    "trip_id": "3D:1401",
                    "iteration_num": 0,
                    "frequencies_defined_trip": False,
                    "trip_short_name": None,
                    "direction_id": 0,
                    "block_id": "3D:303",
                    "route_key": "bc23b5e936992cd318d7b35d3b18462b",
                    "route_id": "3D:374",
                    "route_type": "3",
                    "route_short_name": "374",
                    "route_long_name": "Pittsburg-Bay Point BART / Bay Point",
                    "route_continuous_pickup": None,
                    "route_continuous_drop_off": None,
                    "route_desc": None,
                    "route_color": "A03B00",
                    "route_text_color": "FFFFFF",
                    "agency_id": "3D",
                    "network_id": "3D",
                    "shape_array_key": "2ce3f738fbdcfdf229cab4c19b1dfc08",
                    "shape_id": "3D:25",
                    "contains_warning_duplicate_trip_primary_key": False,
                    "num_distinct_stops_served": 27,
                    "num_stop_times": 28,
                    "trip_first_departure_sec": 39060,
                    "trip_last_arrival_sec": 41160,
                    "trip_start_timezone": "America/Los_Angeles",
                    "trip_end_timezone": "America/Los_Angeles",
                    "service_hours": 0.5833333333333334,
                    "flex_service_hours": None,
                    "contains_warning_duplicate_stop_times_primary_key": False,
                    "contains_warning_missing_foreign_key_stop_id": False,
                    "trip_first_departure_ts": Timestamp("2025-09-01 17:51:00+0000", tz="UTC"),
                    "trip_last_arrival_ts": Timestamp("2025-09-01 18:26:00+0000", tz="UTC"),
                    "first_start_pickup_drop_off_window_sec": None,
                    "last_end_pickup_drop_off_window_sec": None,
                    "is_gtfs_flex_trip": False,
                    "is_entirely_demand_responsive_trip": False,
                    "num_gtfs_flex_stop_times": 0,
                    "num_approximate_timepoint_stop_times": 25,
                    "num_exact_timepoint_stop_times": 3,
                    "num_arrival_times_populated_stop_times": 28,
                    "num_departure_times_populated_stop_times": 28,
                    "trip_first_start_pickup_drop_off_window_ts": None,
                    "trip_last_end_pickup_drop_off_window_ts": None,
                    "trip_start_date_pacific": datetime.date(2025, 9, 1),
                    "trip_first_departure_datetime_pacific": Timestamp("2025-09-01 10:51:00"),
                    "trip_last_arrival_datetime_pacific": Timestamp("2025-09-01 11:26:00"),
                    "trip_start_date_local_tz": datetime.date(2025, 9, 1),
                    "trip_first_departure_datetime_local_tz": Timestamp("2025-09-01 10:51:00"),
                    "trip_last_arrival_datetime_local_tz": Timestamp("2025-09-01 11:26:00"),
                    "trip_first_start_pickup_drop_off_window_date_pacific": None,
                    "trip_first_start_pickup_drop_off_window_datetime_pacific": None,
                    "trip_last_end_pickup_drop_off_window_pacific": None,
                    "trip_first_start_pickup_drop_off_window_date_local_tz": None,
                    "trip_first_start_pickup_drop_off_window_datetime_local_tz": None,
                    "trip_last_end_pickup_drop_off_window_datetime_local_tz": None,
                    "time_of_day": "Midday",
                },
            ]
        )

    @pytest.mark.default_cassette("TestGtfsUtilsV2.test_get_trips.yaml")
    @pytest.mark.vcr
    def test_get_trips_no_metrolink_feed(self, capfd):
        get_trips(selected_date="2025-09-01", operator_feeds=["c86f88ad0b15f5185d073f91f2130285"])
        out, err = capfd.readouterr()

        assert re.search("could not get metrolink feed on 2025-09-01!", out), "The expected text was not printed."

    @pytest.mark.default_cassette("TestGtfsUtilsV2.test_get_trips.yaml")
    @pytest.mark.vcr
    def test_get_trips_get_df_false(self):
        result = get_trips(
            selected_date="2025-09-01", operator_feeds=["c86f88ad0b15f5185d073f91f2130285"], get_df=False
        )

        assert isinstance(result, sqlalchemy.sql.selectable.Select)

    @pytest.mark.vcr
    def test_get_trips_trip_cols(self):
        result = get_trips(
            selected_date="2025-09-01",
            operator_feeds=["c86f88ad0b15f5185d073f91f2130285"],
            trip_cols=["name", "gtfs_dataset_key", "feed_key", "trip_id", "route_id", "route_type"],
        )

        assert len(result) == 3
        assert result.to_dict(orient="records") == unordered(
            [
                {
                    "name": "Bay Area 511 Regional Schedule",
                    "gtfs_dataset_key": "d6ed100168196d507b4ef1c0d111ee72",
                    "feed_key": "c86f88ad0b15f5185d073f91f2130285",
                    "trip_id": "3D:1358",
                    "route_id": "3D:374",
                    "route_type": "3",
                },
                {
                    "name": "Bay Area 511 Regional Schedule",
                    "gtfs_dataset_key": "d6ed100168196d507b4ef1c0d111ee72",
                    "feed_key": "c86f88ad0b15f5185d073f91f2130285",
                    "trip_id": "3D:2388",
                    "route_id": "3D:374",
                    "route_type": "3",
                },
                {
                    "name": "Bay Area 511 Regional Schedule",
                    "gtfs_dataset_key": "d6ed100168196d507b4ef1c0d111ee72",
                    "feed_key": "c86f88ad0b15f5185d073f91f2130285",
                    "trip_id": "3D:1401",
                    "route_id": "3D:374",
                    "route_type": "3",
                },
            ]
        )

    @pytest.mark.vcr
    def test_get_trips_custom_filtering(self):
        result = get_trips(
            selected_date="2025-09-01",
            operator_feeds=["c86f88ad0b15f5185d073f91f2130285"],
            trip_cols=["name", "gtfs_dataset_key", "feed_key", "trip_id", "trip_instance_key"],
            custom_filtering={"trip_instance_key": ["d7d7502d292a35c41ee5a6c3c43f2fd5"]},
        )

        assert len(result) == 1
        assert result.to_dict(orient="records") == [
            {
                "name": "Bay Area 511 Regional Schedule",
                "gtfs_dataset_key": "d6ed100168196d507b4ef1c0d111ee72",
                "feed_key": "c86f88ad0b15f5185d073f91f2130285",
                "trip_id": "3D:1358",
                "trip_instance_key": "d7d7502d292a35c41ee5a6c3c43f2fd5",
            }
        ]

    @pytest.mark.vcr
    def test_get_trips_metrolink_feed_present(self, capfd):
        result = get_trips(selected_date="2025-11-24", operator_feeds=["4321a7e3901b2275805494a746ec1c6a"])

        assert len(result) == 1
        out, err = capfd.readouterr()

        assert re.search("metrolink", out, re.IGNORECASE) is None, "Should not have printed about metrolink feed."

    @pytest.mark.vcr
    def test_get_trips_fill_in_metrolink_shape_id(self, capfd):
        result = get_trips(
            selected_date="2025-11-24",
            operator_feeds=["918ed58c79d05e956cf6f0c15e2a9902"],
            trip_cols=["name", "feed_key", "shape_id"],
        )

        assert len(result) == 1
        # TODO shape_id was already VTin in the DB. It's not clear when fill_in_metrolink_trips_df_with_shape_id is needed.
        assert result.to_dict(orient="records") == [
            {"name": "Metrolink Schedule", "feed_key": "918ed58c79d05e956cf6f0c15e2a9902", "shape_id": "VTin"}
        ]

    def test_get_trips_no_operator_feeds(self):
        with pytest.raises(ValueError, match="Supply list of feed keys or operator names!"):
            get_trips(selected_date="2025-08-23")

    @pytest.mark.vcr
    def test_get_shapes(self):
        result = get_shapes(
            selected_date="2025-10-01",
            operator_feeds=["3ea60aa240ddc543da5415ccc759fd6d", "ebeaafe0a365384015dfe01dd80b683d"],
        )

        assert len(result) == 2
        assert result.to_dict(orient="records") == unordered(
            [
                {
                    # I truncated the pt_array values in the BigQuery test DB to 10 points just to have a succinct example. Typically, there are hundreds of points in a pt_array LineString.
                    "geometry": LineString(
                        [
                            [-120.95261, 39.09977],
                            [-120.95261, 39.09976],
                            [-120.95258, 39.09982],
                            [-120.95239, 39.09974],
                            [-120.95177, 39.09964],
                            [-120.95158, 39.09966],
                            [-120.95109, 39.09787],
                            [-120.95086, 39.09687],
                            [-120.95073, 39.09627],
                            [-120.95071, 39.09604],
                        ]
                    )
                },
                {
                    "geometry": LineString(
                        [
                            [-116.99362, 34.88429],
                            [-116.99375, 34.88388],
                            [-116.99368, 34.88388],
                            [-116.99368, 34.88394],
                            [-116.99368, 34.88437],
                            [-116.99368, 34.88459],
                            [-116.99368, 34.8849],
                            [-116.99376, 34.88502],
                            [-116.99376, 34.88528],
                            [-116.99377, 34.88553],
                        ]
                    )
                },
            ]
        )

    @pytest.mark.vcr
    def test_get_shapes_shape_cols(self):
        result = get_shapes(
            selected_date="2025-10-01",
            operator_feeds=["3ea60aa240ddc543da5415ccc759fd6d"],
            shape_cols=[
                "feed_key",
                "feed_timezone",
                "service_date",
                "shape_first_departure_datetime_pacific",
                "shape_last_arrival_datetime_pacific",
                "shape_id",
                "shape_array_key",
                "n_trips",
            ],
        )
        assert len(result) == 1
        assert result.to_dict(orient="records") == [
            {
                "feed_key": "3ea60aa240ddc543da5415ccc759fd6d",
                "feed_timezone": "America/Los_Angeles",
                "service_date": datetime.date(2025, 10, 1),
                "shape_first_departure_datetime_pacific": Timestamp("2025-10-01 09:45:00"),
                "shape_last_arrival_datetime_pacific": Timestamp("2025-10-01 11:35:00"),
                "shape_id": "2m8h",
                "shape_array_key": "a023425d1b44b2af7ffa58e220b7da8b",
                "n_trips": 1,
                "geometry": LineString(
                    [
                        [-120.95261, 39.09977],
                        [-120.95261, 39.09976],
                        [-120.95258, 39.09982],
                        [-120.95239, 39.09974],
                        [-120.95177, 39.09964],
                        [-120.95158, 39.09966],
                        [-120.95109, 39.09787],
                        [-120.95086, 39.09687],
                        [-120.95073, 39.09627],
                        [-120.95071, 39.09604],
                    ]
                ),
            }
        ]

    @pytest.mark.default_cassette("TestGtfsUtilsV2.test_get_shapes_shape_cols.yaml")
    @pytest.mark.vcr
    def test_get_shapes_crs_esri(self):
        result = get_shapes(
            selected_date="2025-10-01",
            operator_feeds=["3ea60aa240ddc543da5415ccc759fd6d"],
            crs=geography_utils.CA_NAD83Albers_ft,
        )
        assert len(result) == 1

        assert result.geometry.values[0].equals_exact(
            LineString(
                [
                    [-270052.919, 9519794.721],
                    [-270052.956, 9519791.075],
                    [-270044.231, 9519812.866],
                    [-269990.664, 9519783.156],
                    [-269815.273, 9519744.929],
                    [-269761.339, 9519751.681],
                    [-269628.982, 9519097.624],
                    [-269567.438, 9518732.354],
                    [-269532.778, 9518513.214],
                    [-269527.949, 9518429.296],
                ]
            ),
            tolerance=0.001,
        )

    @pytest.mark.default_cassette("TestGtfsUtilsV2.test_get_shapes_shape_cols.yaml")
    @pytest.mark.vcr
    def test_get_shapes_crs_epsg(self):
        result = get_shapes(
            selected_date="2025-10-01",
            operator_feeds=["3ea60aa240ddc543da5415ccc759fd6d"],
            crs=geography_utils.CA_NAD83Albers_m,
        )
        assert len(result) == 1

        assert result.geometry.values[0].equals_exact(
            LineString(
                [
                    [-82312.294, 120841.673],
                    [-82312.305, 120840.561],
                    [-82309.646, 120847.203],
                    [-82293.319, 120838.148],
                    [-82239.860, 120826.496],
                    [-82223.421, 120828.554],
                    [-82183.078, 120629.197],
                    [-82164.319, 120517.863],
                    [-82153.755, 120451.069],
                    [-82152.283, 120425.490],
                ]
            ),
            tolerance=0.001,
        )

    @pytest.mark.vcr
    def test_get_shapes_custom_filtering(self):
        result = get_shapes(
            selected_date="2025-10-01",
            operator_feeds=["89c9390b2669927a67a4594f119986d6"],
            custom_filtering={"shape_array_key": ["166d1656656c24bb26a66f0df49edf1c"], "n_trips": [39]},
        )

        assert len(result) == 1
        assert result.to_dict(orient="records") == [
            {
                "geometry": LineString(
                    [
                        [-122.29491, 37.8045],
                        [-122.29469, 37.80446],
                        [-122.29447, 37.80441],
                        [-122.2944, 37.8044],
                        [-122.29419, 37.80434],
                        [-122.2941, 37.80431],
                        [-122.29403, 37.80429],
                        [-122.29391, 37.80427],
                        [-122.29387, 37.8044],
                        [-122.29381, 37.80457],
                    ]
                )
            }
        ]

    def test_get_shapes_no_operator_feeds(self):
        with pytest.raises(ValueError, match="Supply list of feed keys or operator names!"):
            get_shapes(selected_date="2025-09-19")

    def test_get_shapes_get_df_false(self):
        result = get_shapes(
            selected_date="2025-09-01", operator_feeds=["3ea60aa240ddc543da5415ccc759fd6d"], get_df=False
        )

        assert isinstance(result, sqlalchemy.sql.selectable.Select)
        statement = str(result)
        assert re.search(r"SELECT\s.*pt_array.*\sFROM", statement), "The statement did not include pt_array column."


    @pytest.mark.vcr
    def test_get_stops(self):
        result = get_stops(
            selected_date="2024-07-05",
            operator_feeds=["0007cf02f9eee5014e8dbab1bac07f5b"],
        )

        assert len(result) == 3
        assert isinstance(result, gpd.GeoDataFrame)
        assert result.drop(
            columns=[
                "geometry",
                "_feed_valid_from",
                "first_stop_arrival_datetime_pacific",
                "last_stop_departure_datetime_pacific",
            ]
        ).to_dict(orient="records") == unordered(
            [
                {
                    "key": "36e9a190be944c957bc0d73fb291466e",
                    "service_date": datetime.date(2024, 7, 5),
                    "feed_key": "0007cf02f9eee5014e8dbab1bac07f5b",
                    "stop_id": "20501",
                    "feed_timezone": "America/Los_Angeles",
                    "daily_arrivals": 6,
                    "n_hours_in_service": 5,
                    "arrivals_per_hour_owl": 0.0,
                    "arrivals_per_hour_early_am": 0.0,
                    "arrivals_per_hour_am_peak": 2.0,
                    "arrivals_per_hour_midday": 0.8,
                    "arrivals_per_hour_pm_peak": 0.4,
                    "arrivals_per_hour_evening": 0.0,
                    "arrivals_owl": 0,
                    "arrivals_early_am": 0,
                    "arrivals_am_peak": 6,
                    "arrivals_midday": 4,
                    "arrivals_pm_peak": 2,
                    "arrivals_evening": 0,
                    "route_type_0": 0,
                    "route_type_1": 0,
                    "route_type_2": 0,
                    "route_type_3": 12,
                    "route_type_4": 0,
                    "route_type_5": 0,
                    "route_type_6": 0,
                    "route_type_7": 0,
                    "route_type_11": 0,
                    "route_type_12": 0,
                    "missing_route_type": 0,
                    "contains_warning_duplicate_stop_times_primary_key": False,
                    "contains_warning_duplicate_trip_primary_key": False,
                    "route_id_array": ["562", "564"],
                    "route_type_array": [3],
                    "n_route_types": 1,
                    "contains_warning_duplicate_stop_primary_key": False,
                    "stop_key": "1407791f42a01acfc040496be069673e",
                    "tts_stop_name": None,
                    "parent_station": None,
                    "stop_code": None,
                    "stop_name": "Aberdeen - 150 Tinemaha Road",
                    "stop_desc": None,
                    "location_type": 0,
                    "stop_timezone_coalesced": "America/Los_Angeles",
                    "wheelchair_boarding": 1,
                },
                {
                    "key": "d3b1b52ff685a00becb0d486cd209cd2",
                    "service_date": datetime.date(2024, 7, 5),
                    "feed_key": "0007cf02f9eee5014e8dbab1bac07f5b",
                    "stop_id": "20409",
                    "feed_timezone": "America/Los_Angeles",
                    "daily_arrivals": 53,
                    "n_hours_in_service": 12,
                    "arrivals_per_hour_owl": 0.0,
                    "arrivals_per_hour_early_am": 0.0,
                    "arrivals_per_hour_am_peak": 2.3,
                    "arrivals_per_hour_midday": 3.0,
                    "arrivals_per_hour_pm_peak": 3.0,
                    "arrivals_per_hour_evening": 0.8,
                    "arrivals_owl": 0,
                    "arrivals_early_am": 0,
                    "arrivals_am_peak": 7,
                    "arrivals_midday": 15,
                    "arrivals_pm_peak": 15,
                    "arrivals_evening": 3,
                    "route_type_0": 0,
                    "route_type_1": 0,
                    "route_type_2": 0,
                    "route_type_3": 40,
                    "route_type_4": 0,
                    "route_type_5": 0,
                    "route_type_6": 0,
                    "route_type_7": 0,
                    "route_type_11": 0,
                    "route_type_12": 0,
                    "missing_route_type": 0,
                    "contains_warning_duplicate_stop_times_primary_key": False,
                    "contains_warning_duplicate_trip_primary_key": False,
                    "route_id_array": ["5014"],
                    "route_type_array": [3],
                    "n_route_types": 1,
                    "contains_warning_duplicate_stop_primary_key": False,
                    "stop_key": "41bc1507a378f41c622a18dfde1ceff6",
                    "tts_stop_name": None,
                    "parent_station": None,
                    "stop_code": "AC",
                    "stop_name": "Adventure Center",
                    "stop_desc": None,
                    "location_type": 0,
                    "stop_timezone_coalesced": "America/Los_Angeles",
                    "wheelchair_boarding": 1,
                },
                {
                    "key": "6a9b406f1e940f2f98b40cd9af8e0bdc",
                    "service_date": datetime.date(2024, 7, 5),
                    "feed_key": "0007cf02f9eee5014e8dbab1bac07f5b",
                    "stop_id": "20399",
                    "feed_timezone": "America/Los_Angeles",
                    "daily_arrivals": 52,
                    "n_hours_in_service": 0,
                    "arrivals_per_hour_owl": 0.0,
                    "arrivals_per_hour_early_am": 0.0,
                    "arrivals_per_hour_am_peak": 0.0,
                    "arrivals_per_hour_midday": 0.0,
                    "arrivals_per_hour_pm_peak": 0.0,
                    "arrivals_per_hour_evening": 0.0,
                    "arrivals_owl": 0,
                    "arrivals_early_am": 0,
                    "arrivals_am_peak": 0,
                    "arrivals_midday": 0,
                    "arrivals_pm_peak": 0,
                    "arrivals_evening": 0,
                    "route_type_0": 0,
                    "route_type_1": 0,
                    "route_type_2": 0,
                    "route_type_3": 3,
                    "route_type_4": 0,
                    "route_type_5": 0,
                    "route_type_6": 0,
                    "route_type_7": 0,
                    "route_type_11": 0,
                    "route_type_12": 0,
                    "missing_route_type": 0,
                    "contains_warning_duplicate_stop_times_primary_key": False,
                    "contains_warning_duplicate_trip_primary_key": False,
                    "route_id_array": ["5014"],
                    "route_type_array": [3],
                    "n_route_types": 1,
                    "contains_warning_duplicate_stop_primary_key": False,
                    "stop_key": "acf76129b0b6f4fb964d6f12058373f7",
                    "tts_stop_name": None,
                    "parent_station": None,
                    "stop_code": "R1",
                    "stop_name": "Agnew Meadows",
                    "stop_desc": None,
                    "location_type": 0,
                    "stop_timezone_coalesced": "America/Los_Angeles",
                    "wheelchair_boarding": 1,
                },
            ]
        )

        # Check geometry functions
        assert (
            result.loc[result.key == "36e9a190be944c957bc0d73fb291466e", "geometry"]
            .item()
            .equals_exact(Point(-118.254, 36.977), tolerance=0.001)
        )
        assert (
            result.loc[result.key == "d3b1b52ff685a00becb0d486cd209cd2", "geometry"]
            .item()
            .equals_exact(Point(-119.037, 37.651), tolerance=0.001)
        )
        assert (
            result.loc[result.key == "6a9b406f1e940f2f98b40cd9af8e0bdc", "geometry"]
            .item()
            .equals_exact(Point(-119.081, 37.681), tolerance=0.001)
        )

    @pytest.mark.vcr
    def test_get_stops_stop_cols(self):
        result = get_stops(
            selected_date="2024-07-05",
            operator_feeds=["0007cf02f9eee5014e8dbab1bac07f5b"],
            stop_cols=["key", "stop_id"],
        )

        assert len(result) == 3
        assert isinstance(result, gpd.GeoDataFrame)
        assert result.drop(columns=["geometry"]).to_dict(orient="records") == unordered(
            [
                {
                    "key": "36e9a190be944c957bc0d73fb291466e",
                    "stop_id": "20501",
                },
                {
                    "key": "d3b1b52ff685a00becb0d486cd209cd2",
                    "stop_id": "20409",
                },
                {
                    "key": "6a9b406f1e940f2f98b40cd9af8e0bdc",
                    "stop_id": "20399",
                },
            ]
        )

        assert (
            result.loc[result.key == "36e9a190be944c957bc0d73fb291466e", "geometry"]
            .item()
            .equals_exact(Point(-118.254, 36.977), tolerance=0.001)
        )
        assert (
            result.loc[result.key == "d3b1b52ff685a00becb0d486cd209cd2", "geometry"]
            .item()
            .equals_exact(Point(-119.037, 37.651), tolerance=0.001)
        )
        assert (
            result.loc[result.key == "6a9b406f1e940f2f98b40cd9af8e0bdc", "geometry"]
            .item()
            .equals_exact(Point(-119.081, 37.681), tolerance=0.001)
        )

    @pytest.mark.default_cassette("TestGtfsUtilsV2.test_get_stops.yaml")
    @pytest.mark.vcr
    def test_get_stops_crs_esri(self):
        result = get_stops(
            selected_date="2024-07-05",
            operator_feeds=["0007cf02f9eee5014e8dbab1bac07f5b"],
            crs=geography_utils.CA_NAD83Albers_ft,
        )
        print(result.geometry)

        assert len(result) == 3
        assert isinstance(result, gpd.GeoDataFrame)
        assert (
            result.loc[result.key == "36e9a190be944c957bc0d73fb291466e", "geometry"]
            .item()
            .equals_exact(Point(509159.348, 8749097.078), tolerance=0.001)
        )
        assert (
            result.loc[result.key == "d3b1b52ff685a00becb0d486cd209cd2", "geometry"]
            .item()
            .equals_exact(Point(278278.993, 8991636.925), tolerance=0.001)
        )
        assert (
            result.loc[result.key == "6a9b406f1e940f2f98b40cd9af8e0bdc", "geometry"]
            .item()
            .equals_exact(Point(265634.545, 9002361.211), tolerance=0.001)
        )

    @pytest.mark.default_cassette("TestGtfsUtilsV2.test_get_stops.yaml")
    @pytest.mark.vcr
    def test_get_stops_crs_epsg(self):
        result = get_stops(
            selected_date="2024-07-05",
            operator_feeds=["0007cf02f9eee5014e8dbab1bac07f5b"],
            crs=geography_utils.CA_NAD83Albers_m,
        )

        assert len(result) == 3
        assert isinstance(result, gpd.GeoDataFrame)
        assert (
            result.loc[result.key == "36e9a190be944c957bc0d73fb291466e", "geometry"]
            .item()
            .equals_exact(Point(155192.080, -114067.439), tolerance=0.001)
        )
        assert (
            result.loc[result.key == "d3b1b52ff685a00becb0d486cd209cd2", "geometry"]
            .item()
            .equals_exact(Point(84819.607, -40141.145), tolerance=0.001)
        )
        assert (
            result.loc[result.key == "6a9b406f1e940f2f98b40cd9af8e0bdc", "geometry"]
            .item()
            .equals_exact(Point(80965.571, -36872.377), tolerance=0.001)
        )

    @pytest.mark.vcr
    def test_get_stops_custom_filtering(self):
        result = get_stops(
            selected_date="2024-07-05",
            operator_feeds=["0007cf02f9eee5014e8dbab1bac07f5b"],
            custom_filtering={"stop_id": ["20501"]},
        )

        assert len(result) == 1
        assert isinstance(result, gpd.GeoDataFrame)
        assert result.key.values[0] == "36e9a190be944c957bc0d73fb291466e"
        assert result.stop_id.values[0] == "20501"

    def test_get_stops_no_operator_feeds(self):
        with pytest.raises(ValueError, match="Supply list of feed keys or operator names!"):
            get_stops(selected_date="2024-07-05")

    def test_get_stops_get_df_false(self):
        result = get_stops(
            selected_date="2024-07-05",
            operator_feeds=["0007cf02f9eee5014e8dbab1bac07f5b"],
            get_df=False,
        )

        assert isinstance(result, sqlalchemy.sql.selectable.Select)

    
    @pytest.mark.vcr
    def test_get_stop_times(self, trip):
        result = get_stop_times(
            selected_date="2025-11-24",
            trip_df=trip,
            operator_feeds=["4321a7e3901b2275805494a746ec1c6a"],
        )

        assert len(result) == 2
        assert result.drop(columns=["_feed_valid_from"]).to_dict(orient="records") == unordered(
            [
                {
                    "key": "0a66e67b36bcc42197f05beaed08e373",
                    "_gtfs_key": "70ab2242460f43fcae2e8df8e2650713",
                    "base64_url": "aHR0cHM6Ly9kYXRhLnRyaWxsaXVtdHJhbnNpdC5jb20vZ3Rmcy9wbHVtYXMtY2EtdXMvcGx1bWFzLWNhLXVzLnppcA==",
                    "feed_key": "4321a7e3901b2275805494a746ec1c6a",
                    "trip_id": "(MWF)_t_5405971_b_55349_tn_0",
                    "stop_id": "4160286",
                    "stop_sequence": 0,
                    "arrival_time": "11:00:00",
                    "departure_time": "11:00:00",
                    "arrival_time_interval": relativedelta(hours=+11),
                    "departure_time_interval": relativedelta(hours=+11),
                    "stop_headsign": None,
                    "pickup_type": 0,
                    "drop_off_type": 0,
                    "continuous_pickup": None,
                    "continuous_drop_off": None,
                    "shape_dist_traveled": 0.0,
                    "timepoint": 1,
                    "warning_duplicate_gtfs_key": False,
                    "warning_missing_foreign_key_stop_id": False,
                    "_dt": datetime.date(2025, 8, 22),
                    "_line_number": 1268,
                    "feed_timezone": "America/Los_Angeles",
                    "arrival_sec": 39600,
                    "departure_sec": 39600,
                    "start_pickup_drop_off_window": None,
                    "end_pickup_drop_off_window": None,
                    "start_pickup_drop_off_window_interval": None,
                    "end_pickup_drop_off_window_interval": None,
                    "start_pickup_drop_off_window_sec": None,
                    "end_pickup_drop_off_window_sec": None,
                    "mean_duration_factor": None,
                    "mean_duration_offset": None,
                    "safe_duration_factor": None,
                    "safe_duration_offset": None,
                    "pickup_booking_rule_id": None,
                    "drop_off_booking_rule_id": None,
                    "arrival_hour": 11,
                    "departure_hour": 11,
                },
                {
                    "key": "952a6434038bef86c05f25a2d13f6dda",
                    "_gtfs_key": "aa25f668f6bc26009a51db7b9714b79c",
                    "base64_url": "aHR0cHM6Ly9kYXRhLnRyaWxsaXVtdHJhbnNpdC5jb20vZ3Rmcy9wbHVtYXMtY2EtdXMvcGx1bWFzLWNhLXVzLnppcA==",
                    "feed_key": "4321a7e3901b2275805494a746ec1c6a",
                    "trip_id": "(MWF)_t_5405971_b_55349_tn_0",
                    "stop_id": "18197",
                    "stop_sequence": 1,
                    "arrival_time": "11:30:00",
                    "departure_time": "11:30:00",
                    "arrival_time_interval": relativedelta(hours=+11, minutes=+30),
                    "departure_time_interval": relativedelta(hours=+11, minutes=+30),
                    "stop_headsign": None,
                    "pickup_type": 0,
                    "drop_off_type": 0,
                    "continuous_pickup": None,
                    "continuous_drop_off": None,
                    "shape_dist_traveled": 37.4558,
                    "timepoint": 1,
                    "warning_duplicate_gtfs_key": False,
                    "warning_missing_foreign_key_stop_id": False,
                    "_dt": datetime.date(2025, 8, 22),
                    "_line_number": 1269,
                    "feed_timezone": "America/Los_Angeles",
                    "arrival_sec": 41400,
                    "departure_sec": 41400,
                    "start_pickup_drop_off_window": None,
                    "end_pickup_drop_off_window": None,
                    "start_pickup_drop_off_window_interval": None,
                    "end_pickup_drop_off_window_interval": None,
                    "start_pickup_drop_off_window_sec": None,
                    "end_pickup_drop_off_window_sec": None,
                    "mean_duration_factor": None,
                    "mean_duration_offset": None,
                    "safe_duration_factor": None,
                    "safe_duration_offset": None,
                    "pickup_booking_rule_id": None,
                    "drop_off_booking_rule_id": None,
                    "arrival_hour": 11,
                    "departure_hour": 11,
                },
            ]
        )

    @pytest.mark.vcr
    def test_get_stop_times_stop_time_cols(self, trip):
        result = get_stop_times(
            trip_df=trip,
            operator_feeds=["4321a7e3901b2275805494a746ec1c6a"],
            stop_time_cols=["feed_key"],
        )

        assert len(result) == 2
        assert result.to_dict(orient="records") == unordered(
            [
                {"feed_key": "4321a7e3901b2275805494a746ec1c6a", "arrival_hour": 11, "departure_hour": 11},
                {"feed_key": "4321a7e3901b2275805494a746ec1c6a", "arrival_hour": 11, "departure_hour": 11},
            ]
        )

    @pytest.mark.vcr
    def test_get_stop_times_get_df_false(self, trip):
        result = get_stop_times(trip_df=trip, operator_feeds=["4321a7e3901b2275805494a746ec1c6a"], get_df=False)

        assert isinstance(result, sqlalchemy.sql.selectable.Select)

    @pytest.mark.vcr
    def test_get_stop_times_no_trip_df(self):
        result = get_stop_times(selected_date="2025-11-24", operator_feeds=["918ed58c79d05e956cf6f0c15e2a9902"])

        assert len(result) == 1
        assert result.drop(columns=["_feed_valid_from"]).to_dict(orient="records") == unordered(
            [
                {
                    "key": "af69a8e874fd726f64bcc1a9992ae8ce",
                    "_gtfs_key": "33ab39c71a2559553bf32581d4ae43ee",
                    "base64_url": "aHR0cHM6Ly93d3cubWV0cm9saW5rdHJhaW5zLmNvbS9nbG9iYWxhc3NldHMvYWJvdXQvZ3Rmcy9ndGZzLnppcA==",
                    "feed_key": "918ed58c79d05e956cf6f0c15e2a9902",
                    "trip_id": "294100132",
                    "stop_id": "103",
                    "stop_sequence": 1,
                    "arrival_time": "15:36:00",
                    "departure_time": "15:36:00",
                    "arrival_time_interval": relativedelta(hours=+15, minutes=+36),
                    "departure_time_interval": relativedelta(hours=+15, minutes=+36),
                    "stop_headsign": "L.A. Union Station",
                    "pickup_type": 0,
                    "drop_off_type": None,
                    "continuous_pickup": None,
                    "continuous_drop_off": None,
                    "shape_dist_traveled": None,
                    "timepoint": 1,
                    "warning_duplicate_gtfs_key": False,
                    "warning_missing_foreign_key_stop_id": False,
                    "_dt": datetime.date(2025, 8, 26),
                    "_line_number": 876,
                    "feed_timezone": "America/Los_Angeles",
                    "arrival_sec": 56160,
                    "departure_sec": 56160,
                    "start_pickup_drop_off_window": None,
                    "end_pickup_drop_off_window": None,
                    "start_pickup_drop_off_window_interval": None,
                    "end_pickup_drop_off_window_interval": None,
                    "start_pickup_drop_off_window_sec": None,
                    "end_pickup_drop_off_window_sec": None,
                    "mean_duration_factor": None,
                    "mean_duration_offset": None,
                    "safe_duration_factor": None,
                    "safe_duration_offset": None,
                    "pickup_booking_rule_id": None,
                    "drop_off_booking_rule_id": None,
                    "arrival_hour": 15,
                    "departure_hour": 15,
                }
            ]
        )

    @pytest.mark.vcr
    def test_get_stop_times_custom_filtering(self):
        result = get_stop_times(
            selected_date="2025-11-24",
            operator_feeds=["4321a7e3901b2275805494a746ec1c6a"],
            custom_filtering={"stop_id": ["18197"]},
        )

        assert len(result) == 1
        assert result[["key", "feed_key", "trip_id", "stop_id"]].to_dict(orient="records") == [
            {
                "key": "952a6434038bef86c05f25a2d13f6dda",
                "feed_key": "4321a7e3901b2275805494a746ec1c6a",
                "trip_id": "(MWF)_t_5405971_b_55349_tn_0",
                "stop_id": "18197",
            },
        ]

    def test_get_stop_times_no_operator_feeds(self):
        with pytest.raises(ValueError, match="Supply list of feed keys or operator names!"):
            get_stop_times(selected_date="2025-11-01")

    @pytest.mark.vcr
    def test_filter_to_public_schedule_gtfs_dataset_keys(self):
        result = filter_to_public_schedule_gtfs_dataset_keys()

        assert len(result) == 8
        assert result == unordered(
            "1165b1474df778cb0fc3ba9246e32035",
            "224050930309d60dbfad4b0b1ff8f17f",
            "fe61acc86788295b399a43cb35a9e9c5",
            "ff87c8304a909c02ef5045cc802864ab",
            "372a06b593e1716d1c911b1d1d35bedd",
            "c51dbfdd47838f86074c4ef3179cc9ed",
            "d6ed100168196d507b4ef1c0d111ee72",
            "2f506f822a5f9b2afa48bda762a5e81d",
        )

    @pytest.mark.default_cassette("TestGtfsUtilsV2.test_filter_to_public_schedule_gtfs_dataset_keys.yaml")
    @pytest.mark.vcr
    def test_filter_to_public_schedule_gtfs_dataset_keys_get_df(self):
        result = filter_to_public_schedule_gtfs_dataset_keys(get_df=True)

        assert len(result) == 9
        assert result.to_dict(orient="records") == unordered(
            [
                {
                    "gtfs_dataset_key": "1165b1474df778cb0fc3ba9246e32035",
                    "gtfs_dataset_name": "Amtrak Schedule",
                    "private_dataset": None,
                },
                {
                    "gtfs_dataset_key": "224050930309d60dbfad4b0b1ff8f17f",
                    "gtfs_dataset_name": "Alhambra Schedule",
                    "private_dataset": None,
                },
                {
                    "gtfs_dataset_key": "fe61acc86788295b399a43cb35a9e9c5",
                    "gtfs_dataset_name": "Metrolink Schedule",
                    "private_dataset": None,
                },
                {
                    "gtfs_dataset_key": "ff87c8304a909c02ef5045cc802864ab",
                    "gtfs_dataset_name": "Metrolink Schedule",
                    "private_dataset": None,
                },
                {
                    "gtfs_dataset_key": "ff87c8304a909c02ef5045cc802864ab",
                    "gtfs_dataset_name": "Metrolink Schedule",
                    "private_dataset": None,
                },
                {
                    "gtfs_dataset_key": "372a06b593e1716d1c911b1d1d35bedd",
                    "gtfs_dataset_name": "Santa Ynez Mecatran Schedule",
                    "private_dataset": None,
                },
                {
                    "gtfs_dataset_key": "c51dbfdd47838f86074c4ef3179cc9ed",
                    "gtfs_dataset_name": "Santa Ynez Mecatran Schedule",
                    "private_dataset": None,
                },
                {
                    "gtfs_dataset_key": "d6ed100168196d507b4ef1c0d111ee72",
                    "gtfs_dataset_name": "Bay Area 511 Regional Schedule",
                    "private_dataset": None,
                },
                {
                    "gtfs_dataset_key": "2f506f822a5f9b2afa48bda762a5e81d",
                    "gtfs_dataset_name": "AC Transit Schedule",
                    "private_dataset": None,
                },
            ]
        )
