import datetime
import re

import pytest
import sqlalchemy
from pandas._libs.tslibs.timestamps import Timestamp
from pytest_unordered import unordered
from shared_utils.gtfs_utils_v2 import (
    get_metrolink_feed_key,
    get_trips,
    schedule_daily_feed_to_gtfs_dataset_name,
)


class TestGtfsUtilsV2:
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
