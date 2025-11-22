import pandas as pd
import pytest
import sqlalchemy
from pytest_unordered import unordered
from shared_utils.schedule_rt_utils import (
    filter_dim_county_geography,
    filter_dim_gtfs_datasets,
    filter_dim_organizations,
    get_organization_id,
    get_schedule_gtfs_dataset_key,
    sample_gtfs_dataset_key_to_organization_crosswalk,
)


class TestScheduleRtUtils:
    @pytest.mark.vcr
    def test_get_schedule_gtfs_dataset_key(self):
        result = get_schedule_gtfs_dataset_key(date="2025-09-01")

        assert len(result) == 1
        assert result.gtfs_dataset_key.values[0] == "0089bd1b0a2b78a8590d8749737d7146"
        assert result.feed_key.values[0] == "10f6bb537140b7e52c8f313f3d611f71"

    @pytest.mark.vcr
    def test_filter_dim_gtfs_datasets(self):
        result = filter_dim_gtfs_datasets()

        assert len(result) == 3
        assert result.to_dict(orient="records") == unordered(
            [
                {
                    "gtfs_dataset_key": "c51dbfdd47838f86074c4ef3179cc9ed",
                    "gtfs_dataset_name": "Santa Ynez Mecatran Schedule",
                    "type": "schedule",
                    "regional_feed_type": None,
                    "uri": "http://app.mecatran.com/urb/ws/feed/c2l0ZT1zeXZ0O2NsaWVudD1zZWxmO2V4cGlyZT07dHlwZT1ndGZzO2tleT00MjcwNzQ0ZTY4NTAzOTMyMDIxMDdjNzI0MDRkMzYyNTM4MzI0YzI0",
                    "base64_url": "aHR0cDovL2FwcC5tZWNhdHJhbi5jb20vdXJiL3dzL2ZlZWQvYzJsMFpUMXplWFowTzJOc2FXVnVkRDF6Wld4bU8yVjRjR2x5WlQwN2RIbHdaVDFuZEdaek8ydGxlVDAwTWpjd056UTBaVFk0TlRBek9UTXlNREl4TURkak56STBNRFJrTXpZeU5UTTRNekkwWXpJMA==",
                },
                {
                    "gtfs_dataset_key": "372a06b593e1716d1c911b1d1d35bedd",
                    "gtfs_dataset_name": "Santa Ynez Mecatran Schedule",
                    "type": "schedule",
                    "regional_feed_type": None,
                    "uri": "http://app.mecatran.com/urb/ws/feed/c2l0ZT1zeXZ0O2NsaWVudD1zZWxmO2V4cGlyZT07dHlwZT1ndGZzO2tleT00MjcwNzQ0ZTY4NTAzOTMyMDIxMDdjNzI0MDRkMzYyNTM4MzI0YzI0",
                    "base64_url": "aHR0cDovL2FwcC5tZWNhdHJhbi5jb20vdXJiL3dzL2ZlZWQvYzJsMFpUMXplWFowTzJOc2FXVnVkRDF6Wld4bU8yVjRjR2x5WlQwN2RIbHdaVDFuZEdaek8ydGxlVDAwTWpjd056UTBaVFk0TlRBek9UTXlNREl4TURkak56STBNRFJrTXpZeU5UTTRNekkwWXpJMA==",
                },
                {
                    "gtfs_dataset_key": "8aed0709366badf9342e03d0a2d72b8d",
                    "gtfs_dataset_name": "SLO Trip Updates",
                    "type": "trip_updates",
                    "regional_feed_type": None,
                    "uri": "http://data.peaktransit.com/gtfsrt/1/TripUpdate.pb",
                    "base64_url": "aHR0cDovL2RhdGEucGVha3RyYW5zaXQuY29tL2d0ZnNydC8xL1RyaXBVcGRhdGUucGI=",
                },
            ]
        )

    def test_filter_dim_gtfs_datasets_get_df_false(self):
        result = filter_dim_gtfs_datasets(get_df=False)

        assert isinstance(result, sqlalchemy.sql.selectable.Select)

    @pytest.mark.vcr
    def test_filter_dim_gtfs_datasets_keep_cols_subset(self):
        result = filter_dim_gtfs_datasets(keep_cols=["key", "name"])

        assert len(result) == 3
        assert result.to_dict(orient="records") == unordered(
            [
                {
                    "gtfs_dataset_key": "c51dbfdd47838f86074c4ef3179cc9ed",
                    "gtfs_dataset_name": "Santa Ynez Mecatran Schedule",
                },
                {
                    "gtfs_dataset_key": "372a06b593e1716d1c911b1d1d35bedd",
                    "gtfs_dataset_name": "Santa Ynez Mecatran Schedule",
                },
                {"gtfs_dataset_key": "8aed0709366badf9342e03d0a2d72b8d", "gtfs_dataset_name": "SLO Trip Updates"},
            ]
        )

    @pytest.mark.vcr
    def test_filter_dim_gtfs_datasets_custom_filtering(self):
        result = filter_dim_gtfs_datasets(custom_filtering={"type": ["trip_updates"]})

        assert len(result) == 1
        assert result.to_dict(orient="records") == unordered(
            [
                {
                    "gtfs_dataset_key": "8aed0709366badf9342e03d0a2d72b8d",
                    "gtfs_dataset_name": "SLO Trip Updates",
                    "type": "trip_updates",
                    "regional_feed_type": None,
                    "uri": "http://data.peaktransit.com/gtfsrt/1/TripUpdate.pb",
                    "base64_url": "aHR0cDovL2RhdGEucGVha3RyYW5zaXQuY29tL2d0ZnNydC8xL1RyaXBVcGRhdGUucGI=",
                }
            ]
        )

    def test_filter_dim_gtfs_datasets_keep_cols_key_missing(self):
        with pytest.raises(KeyError, match="Include key in keep_cols list"):
            filter_dim_gtfs_datasets(keep_cols=["name", "type", "regional_feed_type", "uri", "base64_url"])

    @pytest.mark.vcr
    def test_get_organization_id_no_merge_cols(self):
        dataframe = pd.DataFrame(
            data=[
                {
                    "feed_key": "bc76f45fb4d8a3c1be8349ad3d085c3c",
                    "schedule_gtfs_dataset_key": "372a06b593e1716d1c911b1d1d35bedd",
                    "schedule_gtfs_dataset_name": "Santa Ynez Mecatran Schedule",
                    "type": "schedule",
                    "schedule_source_record_id": "recuWhPXfxMatv6rL",
                    "regional_feed_type": None,
                    "base64_url": "anything",
                    "uri": "http://www.example.com",
                }
            ]
        )

        with pytest.raises(IndexError, match="list index out of range"):
            get_organization_id(df=dataframe, date="2025-10-12")

    def test_get_organization_id_invalid_merge_cols(self):
        with pytest.raises(KeyError, match="Unable to detect which GTFS quartet"):
            get_organization_id(df=pd.DataFrame(), date="2025-10-12", merge_cols=["notreal"])

    @pytest.mark.vcr
    def test_filter_dim_county_geography(self):
        result = filter_dim_county_geography(date="2025-06-17")

        assert len(result) == 2
        assert result.to_dict(orient="records") == unordered(
            [
                {
                    "organization_name": "City of Rosemead",
                    "caltrans_district": "07 - Los Angeles / Ventura",
                },
                {"organization_name": "Via / Remix Inc.", "caltrans_district": "07 - Los Angeles / Ventura"},
            ]
        )

    @pytest.mark.vcr
    def test_filter_dim_county_geography_date_unavailable(self):
        result = filter_dim_county_geography(date="2024-01-17")

        assert len(result) == 0

    @pytest.mark.vcr
    def test_filter_dim_county_geography_additional_keep_cols(self):
        result = filter_dim_county_geography(
            date="2025-06-17",
            keep_cols=["caltrans_district", "county_geography_name", "msa", "fips"],
        )

        assert len(result) == 2
        assert result.to_dict(orient="records") == unordered(
            [
                {
                    "organization_name": "City of Rosemead",
                    "caltrans_district": "07 - Los Angeles / Ventura",
                    "county_geography_name": "Los Angeles",
                    "msa": "Los Angeles-Long Beach-Anaheim",
                    "fips": 6037,
                },
                {
                    "organization_name": "Via / Remix Inc.",
                    "caltrans_district": "07 - Los Angeles / Ventura",
                    "county_geography_name": "Los Angeles",
                    "msa": "Los Angeles-Long Beach-Anaheim",
                    "fips": 6037,
                },
            ]
        )

        assert result.columns.is_unique == True

    @pytest.mark.vcr
    def test_filter_dim_organizations(self):
        result = filter_dim_organizations()

        assert len(result) == 3
        assert result.to_dict(orient="records") == unordered(
            [
                {"organization_source_record_id": "reckGS8egMZryjbX7"},
                {"organization_source_record_id": "recyqZ1zbZMkeA7Vf"},
                {"organization_source_record_id": "recOT4QO6t6mRhUEu"},
            ]
        )
        assert "coolplace33333bX7" not in result.organization_source_record_id.values

    @pytest.mark.vcr
    def test_filter_dim_organizations_additional_keep_cols(self):
        result = filter_dim_organizations(keep_cols=["key", "name", "organization_type"])

        assert len(result) == 3
        assert result.to_dict(orient="records") == unordered(
            [
                {
                    "key": "35448956533b3ff4f8c9cf4e7886c974",
                    "name": "City of Mission Viejo",
                    "organization_type": "City/Town",
                },
                {
                    "key": "02a0e06b1ddb80e5695fc82fcc0c3ccc",
                    "name": "City of Patterson",
                    "organization_type": "City/Town",
                },
                {
                    "key": "4cb90bb76f9cd9472a2df6dd9014b4fa",
                    "name": "City of Chula Vista",
                    "organization_type": "City/Town",
                },
            ]
        )

    @pytest.mark.vcr
    def test_filter_dim_organizations_custom_filtering(self):
        result = filter_dim_organizations(
            custom_filtering={"name": ["City of Mission Viejo", "City of Patterson"]},
            keep_cols=["name", "source_record_id"],
        )

        assert len(result) == 2
        assert result.to_dict(orient="records") == unordered(
            [
                {"name": "City of Mission Viejo", "organization_source_record_id": "reckGS8egMZryjbX7"},
                {"name": "City of Patterson", "organization_source_record_id": "recyqZ1zbZMkeA7Vf"},
            ]
        )

    @pytest.mark.vcr
    def test_sample_gtfs_dataset_key_to_organization_crosswalk(self):
        dataframe = pd.DataFrame(
            data=[
                {
                    "gtfs_dataset_key": "372a06b593e1716d1c911b1d1d35bedd",
                    "feed_key": "bc76f45fb4d8a3c1be8349ad3d085c3c",
                    "name": "Santa Ynez Mecatran Schedule",
                }
            ]
        )

        result = sample_gtfs_dataset_key_to_organization_crosswalk(
            df=dataframe,
            date="2025-10-16",
            quartet_data="schedule",
        )

        assert len(result) == 1
        assert result.to_dict(orient="records") == unordered(
            [
                {
                    "feed_key": "bc76f45fb4d8a3c1be8349ad3d085c3c",
                    "schedule_gtfs_dataset_key": "372a06b593e1716d1c911b1d1d35bedd",
                    "schedule_gtfs_dataset_name": "Santa Ynez Mecatran Schedule",
                    "type": "schedule",
                    "schedule_source_record_id": "recuWhPXfxMatv6rL",
                    "regional_feed_type": None,
                    "base64_url": "aHR0cDovL2FwcC5tZWNhdHJhbi5jb20vdXJiL3dzL2ZlZWQvYzJsMFpUMXplWFowTzJOc2FXVnVkRDF6Wld4bU8yVjRjR2x5WlQwN2RIbHdaVDFuZEdaek8ydGxlVDAwTWpjd056UTBaVFk0TlRBek9UTXlNREl4TURkak56STBNRFJrTXpZeU5UTTRNekkwWXpJMA==",
                    "uri": "http://app.mecatran.com/urb/ws/feed/c2l0ZT1zeXZ0O2NsaWVudD1zZWxmO2V4cGlyZT07dHlwZT1ndGZzO2tleT00MjcwNzQ0ZTY4NTAzOTMyMDIxMDdjNzI0MDRkMzYyNTM4MzI0YzI0",
                    "organization_source_record_id": "reckp33bhAuZlmO1M",
                    "organization_name": "Santa Ynez Band of Chumash Mission Indians of the Santa Ynez Reservation, California",
                    "caltrans_district": "05 - San Luis Obispo / Santa Barbara",
                }
            ]
        )

    @pytest.mark.vcr
    def test_sample_gtfs_dataset_key_to_organization_crosswalk_subset_gtfs_dataset_cols(self):
        dataframe = pd.DataFrame(
            data=[
                {
                    "gtfs_dataset_key": "372a06b593e1716d1c911b1d1d35bedd",
                    "feed_key": "bc76f45fb4d8a3c1be8349ad3d085c3c",
                    "name": "Santa Ynez Mecatran Schedule",
                }
            ]
        )

        result = sample_gtfs_dataset_key_to_organization_crosswalk(
            df=dataframe,
            date="2025-10-16",
            quartet_data="schedule",
            dim_gtfs_dataset_cols=[
                "key",
                "name",
                "source_record_id",
            ],
        )

        assert len(result) == 1
        assert result.to_dict(orient="records") == unordered(
            [
                {
                    "feed_key": "bc76f45fb4d8a3c1be8349ad3d085c3c",
                    "schedule_gtfs_dataset_key": "372a06b593e1716d1c911b1d1d35bedd",
                    "schedule_gtfs_dataset_name": "Santa Ynez Mecatran Schedule",
                    "schedule_source_record_id": "recuWhPXfxMatv6rL",
                    "organization_source_record_id": "reckp33bhAuZlmO1M",
                    "organization_name": "Santa Ynez Band of Chumash Mission Indians of the Santa Ynez Reservation, California",
                    "caltrans_district": "05 - San Luis Obispo / Santa Barbara",
                }
            ]
        )
