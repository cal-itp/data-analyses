import pytest
import sqlalchemy
from pytest_unordered import unordered
from shared_utils.gtfs_utils_v2 import (
    get_metrolink_feed_key,
    schedule_daily_feed_to_gtfs_dataset_name,
)


class TestGtfsUtilsV2:
    @pytest.fixture
    def project(self):
        return "cal-itp-data-infra-staging"

    @pytest.fixture
    def dataset(self):
        return "test_shared_utils"

    @pytest.mark.vcr
    def test_get_metrolink_feed_key(self, project: str, dataset: str):
        result = get_metrolink_feed_key(
            project=project, dataset=dataset, transit_dataset=dataset, selected_date="2025-08-23"
        )

        assert result == "0b0ebeff0c1f7ff681e6a06d6218ecd6"

    @pytest.mark.default_cassette("TestGtfsUtilsV2.test_get_metrolink_feed_key.yaml")
    @pytest.mark.vcr
    def test_get_metrolink_feed_key_get_df(self, project: str, dataset: str):
        result = get_metrolink_feed_key(
            project=project, dataset=dataset, transit_dataset=dataset, selected_date="2025-08-23", get_df=True
        )

        assert len(result) == 1
        assert result.to_dict(orient="records") == [
            {"feed_key": "0b0ebeff0c1f7ff681e6a06d6218ecd6", "name": "Metrolink Schedule"}
        ]

    @pytest.mark.vcr
    def test_schedule_daily_feed_to_gtfs_dataset_name(self, project: str, dataset: str):
        result = schedule_daily_feed_to_gtfs_dataset_name(
            project=project, dataset=dataset, transit_dataset=dataset, selected_date="2025-09-01"
        )

        assert len(result) == 1
        assert result.to_dict(orient="records") == [
            {
                "gtfs_dataset_key": "9f2566d34fde8d7d0d51b64bdf77a7ba",
                "gtfs_dataset_name": "Bay Area 511 ACE Schedule",
                "type": "schedule",
                "regional_feed_type": "Regional Subfeed",
                "name": "Bay Area 511 ACE Schedule",
            }
        ]

    @pytest.mark.default_cassette("TestGtfsUtilsV2.test_schedule_daily_feed_to_gtfs_dataset_name.yaml")
    @pytest.mark.vcr
    def test_schedule_daily_feed_to_gtfs_dataset_name_get_df_false(self, project: str, dataset: str):
        result = schedule_daily_feed_to_gtfs_dataset_name(
            project=project, dataset=dataset, transit_dataset=dataset, selected_date="2025-09-01", get_df=False
        )

        assert isinstance(result, sqlalchemy.sql.selectable.Select)

    @pytest.mark.vcr
    def test_schedule_daily_feed_to_gtfs_dataset_name_keep_cols(self, project: str, dataset: str):
        result = schedule_daily_feed_to_gtfs_dataset_name(
            project=project,
            dataset=dataset,
            transit_dataset=dataset,
            selected_date="2025-09-01",
            keep_cols=["name", "gtfs_dataset_key", "feed_key"],
        )

        assert len(result) == 1
        assert result.to_dict(orient="records") == [
            {
                "gtfs_dataset_key": "9f2566d34fde8d7d0d51b64bdf77a7ba",
                "name": "Bay Area 511 ACE Schedule",
                "feed_key": "3bbe132a97a1510d3e1a265875ef7590",
            }
        ]

    @pytest.mark.vcr
    def test_schedule_daily_feed_to_gtfs_dataset_name_feed_option_customer_facing(self, project: str, dataset: str):
        result = schedule_daily_feed_to_gtfs_dataset_name(
            project=project,
            dataset=dataset,
            transit_dataset=dataset,
            selected_date="2025-09-01",
            feed_option="customer_facing",
        )

        assert len(result) == 1
        assert result.to_dict(orient="records") == [
            {
                "gtfs_dataset_key": "d6ed100168196d507b4ef1c0d111ee72",
                "gtfs_dataset_name": "Bay Area 511 Regional Schedule",
                "type": "schedule",
                "regional_feed_type": "Combined Regional Feed",
                "name": "Bay Area 511 Regional Schedule",
            }
        ]

    @pytest.mark.vcr
    def test_schedule_daily_feed_to_gtfs_dataset_name_feed_option_current_feeds(self, project: str, dataset: str):
        result = schedule_daily_feed_to_gtfs_dataset_name(
            project=project,
            dataset=dataset,
            transit_dataset=dataset,
            selected_date="2025-09-01",
            feed_option="current_feeds",
        )

        assert len(result) == 2
        assert result.to_dict(orient="records") == unordered(
            [
                {
                    "gtfs_dataset_key": "d6ed100168196d507b4ef1c0d111ee72",
                    "gtfs_dataset_name": "Bay Area 511 Regional Schedule",
                    "type": "schedule",
                    "regional_feed_type": "Combined Regional Feed",
                    "name": "Bay Area 511 Regional Schedule",
                },
                {
                    "gtfs_dataset_key": "9f2566d34fde8d7d0d51b64bdf77a7ba",
                    "gtfs_dataset_name": "Bay Area 511 ACE Schedule",
                    "type": "schedule",
                    "regional_feed_type": "Regional Subfeed",
                    "name": "Bay Area 511 ACE Schedule",
                },
            ]
        )

    @pytest.mark.vcr
    def test_schedule_daily_feed_to_gtfs_dataset_name_feed_option_include_precursor(self, project: str, dataset: str):
        result = schedule_daily_feed_to_gtfs_dataset_name(
            project=project,
            dataset=dataset,
            transit_dataset=dataset,
            selected_date="2025-09-01",
            feed_option="include_precursor",
        )

        assert len(result) == 4
        assert result.to_dict(orient="records") == unordered(
            [
                {
                    "gtfs_dataset_key": "2f506f822a5f9b2afa48bda762a5e81d",
                    "gtfs_dataset_name": "AC Transit Schedule",
                    "type": "schedule",
                    "regional_feed_type": "Regional Precursor Feed",
                    "name": "AC Transit Schedule",
                },
                {
                    "gtfs_dataset_key": "1165b1474df778cb0fc3ba9246e32035",
                    "gtfs_dataset_name": "Amtrak Schedule",
                    "type": "schedule",
                    "regional_feed_type": None,
                    "name": "Amtrak Schedule",
                },
                {
                    "gtfs_dataset_key": "d6ed100168196d507b4ef1c0d111ee72",
                    "gtfs_dataset_name": "Bay Area 511 Regional Schedule",
                    "type": "schedule",
                    "regional_feed_type": "Combined Regional Feed",
                    "name": "Bay Area 511 Regional Schedule",
                },
                {
                    "gtfs_dataset_key": "9f2566d34fde8d7d0d51b64bdf77a7ba",
                    "gtfs_dataset_name": "Bay Area 511 ACE Schedule",
                    "type": "schedule",
                    "regional_feed_type": "Regional Subfeed",
                    "name": "Bay Area 511 ACE Schedule",
                },
            ]
        )
