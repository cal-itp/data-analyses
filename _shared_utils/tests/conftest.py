import sys

import pytest


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
    }
