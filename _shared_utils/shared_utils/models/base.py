import os
import sys


def get_table_name(dataset, table):
    if hasattr(sys, "_called_from_test"):
        if "TEST_DATASET" in os.environ:
            return f"{os.environ['TEST_DATASET']}.{table}"
        else:
            raise KeyError("You must set TEST_DATASET in your conftest.py to use these models in tests.")

    else:
        return f"{dataset}.{table}"
