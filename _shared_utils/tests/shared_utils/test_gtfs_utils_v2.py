import pytest
import sqlalchemy
from pandas._libs.tslibs.timestamps import Timestamp
from pytest_unordered import unordered
from shared_utils.gtfs_utils_v2 import (
    get_metrolink_feed_key,
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
