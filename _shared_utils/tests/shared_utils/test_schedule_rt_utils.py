import pytest
from pytest_unordered import unordered
from shared_utils.schedule_rt_utils import get_schedule_gtfs_dataset_key, filter_dim_gtfs_datasets

class TestScheduleRtUtils:
    @pytest.fixture
    def project(self):
        return "cal-itp-data-infra-staging"

    @pytest.fixture
    def dataset(self):
        return "test_shared_utils"

    @pytest.mark.vcr
    def test_get_schedule_gtfs_dataset_key(self, project: str, dataset: str):
        result = get_schedule_gtfs_dataset_key(project=project, dataset=dataset, date="2025-09-01")

        assert len(result) == 1
        assert result.gtfs_dataset_key.values[0] == "0089bd1b0a2b78a8590d8749737d7146"
        assert result.feed_key.values[0] == "10f6bb537140b7e52c8f313f3d611f71"

    @pytest.mark.vcr
    def test_filter_dim_gtfs_dataset(self, project: str, dataset: str):
        result = filter_dim_gtfs_datasets(project=project, dataset=dataset)

        assert len(result) == 3
        assert result.to_dict(orient='records') == unordered([{'gtfs_dataset_key': 'c51dbfdd47838f86074c4ef3179cc9ed',
                                                               'gtfs_dataset_name': 'Santa Ynez Mecatran Schedule',
                                                               'type': 'schedule',
                                                               'regional_feed_type': None,
                                                               'uri': 'http://app.mecatran.com/urb/ws/feed/c2l0ZT1zeXZ0O2NsaWVudD1zZWxmO2V4cGlyZT07dHlwZT1ndGZzO2tleT00MjcwNzQ0ZTY4NTAzOTMyMDIxMDdjNzI0MDRkMzYyNTM4MzI0YzI0',
                                                               'base64_url': 'aHR0cDovL2FwcC5tZWNhdHJhbi5jb20vdXJiL3dzL2ZlZWQvYzJsMFpUMXplWFowTzJOc2FXVnVkRDF6Wld4bU8yVjRjR2x5WlQwN2RIbHdaVDFuZEdaek8ydGxlVDAwTWpjd056UTBaVFk0TlRBek9UTXlNREl4TURkak56STBNRFJrTXpZeU5UTTRNekkwWXpJMA=='},
                                                              {'gtfs_dataset_key': '372a06b593e1716d1c911b1d1d35bedd',
                                                               'gtfs_dataset_name': 'Santa Ynez Mecatran Schedule',
                                                               'type': 'schedule',
                                                               'regional_feed_type': None,
                                                               'uri': 'http://app.mecatran.com/urb/ws/feed/c2l0ZT1zeXZ0O2NsaWVudD1zZWxmO2V4cGlyZT07dHlwZT1ndGZzO2tleT00MjcwNzQ0ZTY4NTAzOTMyMDIxMDdjNzI0MDRkMzYyNTM4MzI0YzI0',
                                                               'base64_url': 'aHR0cDovL2FwcC5tZWNhdHJhbi5jb20vdXJiL3dzL2ZlZWQvYzJsMFpUMXplWFowTzJOc2FXVnVkRDF6Wld4bU8yVjRjR2x5WlQwN2RIbHdaVDFuZEdaek8ydGxlVDAwTWpjd056UTBaVFk0TlRBek9UTXlNREl4TURkak56STBNRFJrTXpZeU5UTTRNekkwWXpJMA=='},
                                                              {'gtfs_dataset_key': '8aed0709366badf9342e03d0a2d72b8d',
                                                               'gtfs_dataset_name': 'SLO Trip Updates',
                                                               'type': 'trip_updates',
                                                               'regional_feed_type': None,
                                                               'uri': 'http://data.peaktransit.com/gtfsrt/1/TripUpdate.pb',
                                                               'base64_url': 'aHR0cDovL2RhdGEucGVha3RyYW5zaXQuY29tL2d0ZnNydC8xL1RyaXBVcGRhdGUucGI='}])

    @pytest.mark.vcr
    def test_filter_dim_gtfs_dataset_keep_cols_subset(self, project: str, dataset: str):
        result = filter_dim_gtfs_datasets(keep_cols=["key", "name"], project=project, dataset=dataset)

        assert len(result) == 3
        assert result.to_dict(orient='records') == unordered(
            [{'gtfs_dataset_key': 'c51dbfdd47838f86074c4ef3179cc9ed',
              'gtfs_dataset_name': 'Santa Ynez Mecatran Schedule'},
             {'gtfs_dataset_key': '372a06b593e1716d1c911b1d1d35bedd',
              'gtfs_dataset_name': 'Santa Ynez Mecatran Schedule'},
             {'gtfs_dataset_key': '8aed0709366badf9342e03d0a2d72b8d',
              'gtfs_dataset_name': 'SLO Trip Updates'}])

    @pytest.mark.vcr
    def test_filter_dim_gtfs_dataset_custom_filtering(self, project: str, dataset: str):
        result = filter_dim_gtfs_datasets(custom_filtering={"type": ["trip_updates"]}, project=project, dataset=dataset)

        assert len(result) == 1
        assert result.to_dict(orient='records') == unordered([{'gtfs_dataset_key': '8aed0709366badf9342e03d0a2d72b8d',
                                                               'gtfs_dataset_name': 'SLO Trip Updates',
                                                               'type': 'trip_updates',
                                                               'regional_feed_type': None,
                                                               'uri': 'http://data.peaktransit.com/gtfsrt/1/TripUpdate.pb',
                                                               'base64_url': 'aHR0cDovL2RhdGEucGVha3RyYW5zaXQuY29tL2d0ZnNydC8xL1RyaXBVcGRhdGUucGI='}])
