import pytest
from shared_utils.gtfs_utils_v2 import get_metrolink_feed_key


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
