import pytest
from shared_utils.schedule_rt_utils import get_schedule_gtfs_dataset_key

class TestScheduleRtUtils:
    @pytest.fixture
    def project(self):
        return "cal-itp-data-infra-staging"

    @pytest.fixture
    def dataset(self):
        return "test_mart_gtfs"

    def test_get_schedule_gtfs_dataset_key(self, project: str, dataset: str):
        # this test depends on a table available at cal-itp-data-infra-staging.test_mart_gtfs that you can create with
        # this query and then adjust the test date filter below to match the data you created.
        # CREATE TABLE test_mart_gtfs.fct_daily_feed_scheduled_service_summary AS(
        #     SELECT * FROM mart_gtfs.fct_daily_feed_scheduled_service_summary LIMIT 3
        # );
        result = get_schedule_gtfs_dataset_key(project=project, dataset=dataset, date="2025-09-01")

        assert len(result) == 1
        assert result.gtfs_dataset_key.values[0] == "0089bd1b0a2b78a8590d8749737d7146"
        assert result.feed_key.values[0] == "10f6bb537140b7e52c8f313f3d611f71"
