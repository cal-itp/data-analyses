from retrospective_feed_generation.gtfs_utils import seconds_to_gtfs_format_time
import pytest
import pandas as pd

def test_seconds_to_gtfs_format_time():
    """
    Test that seconds_to_gtfs_format_time works as expected in a simple scenario
    (this is an example test, that does not reflect the priorities of other tests in this directory)
    """
    test_input = pd.Series([65])
    expected_output = pd.Series(["00:01:05"])
    actual_output = seconds_to_gtfs_format_time(test_input)
    assert (expected_output == actual_output).all()