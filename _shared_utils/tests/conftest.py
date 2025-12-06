import json
import sys

import pytest
from vcr.request import Request


def pytest_configure(config):
    sys._called_from_test = True


def pytest_unconfigure(config):
    del sys._called_from_test


@pytest.fixture(scope="module")
def vcr_config():
    return {
        "filter_headers": [
            ("cookie", "FILTERED"),
            ("Authorization", "FILTERED"),
            ("apikey", "FILTERED"),
            ("X-CKAN-API-Key", "FILTERED"),
        ],
        "ignore_hosts": [
            "run-actions-1-azure-eastus.actions.githubusercontent.com",
            "run-actions-2-azure-eastus.actions.githubusercontent.com",
            "run-actions-3-azure-eastus.actions.githubusercontent.com",
            "sts.googleapis.com",
            "iamcredentials.googleapis.com",
            "oauth2.googleapis.com",
        ],
        "match_on": ["method", "scheme", "host", "port", "path", "query", "db_query_body"],
    }


def db_query_body_matcher(request_1: Request, request_2: Request):
    unique_bodies = list({request_1.body, request_2.body})

    if len(unique_bodies) == 1 and unique_bodies[0] is None:
        return True
    elif None in unique_bodies:
        assert False, f"Request bodies cannot be compared.\n Expected: {request_2.body}\n Actual: {request_1.body}"
    else:
        body_1 = json.loads(request_1.body)
        body_2 = json.loads(request_2.body)

        excluded_body_entries = ["requestId"]

        for key, value in body_1.items():
            if key not in excluded_body_entries:
                assert (
                    value == body_2[key]
                ), f"Request bodies do not match on {key}.\n Expected: {body_2[key]}\n Actual: {value}"

        return True


def pytest_recording_configure(config, vcr):
    vcr.register_matcher("db_query_body", db_query_body_matcher)
