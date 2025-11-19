import sys


def get_table_name(dataset, table):
    if hasattr(sys, "_called_from_test"):
        return f"test_shared_utils.{table}"
    else:
        return f"{dataset}.{table}"
