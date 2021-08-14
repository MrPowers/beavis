import pytest
import beavis
import pandas as pd
import dask.dataframe as dd


def describe_print_partitions():
    def it_prints_values_in_all_partitions():
        df = pd.DataFrame(
            {"nums": [1, 2, 3, 4, 5, 6], "letters": ["a", "b", "c", "d", "e", "f"]}
        )
        ddf = dd.from_pandas(df, npartitions=2)
        beavis.print_partitions(ddf)


def describe_print_dtypes():
    def it_prints_dtypes_in_code_format():
        df = pd.DataFrame(
            {"nums": [1, 2, 3, 4, 5, 6], "letters": ["a", "b", "c", "d", "e", "f"]}
        )
        ddf = dd.from_pandas(df, npartitions=2)
        assert ddf.npartitions == 2
        beavis.print_dtypes(ddf)


def describe_create_ddf():
    def it_creates_a_ddf():
        df = pd.DataFrame({"nums": [1, 2, 3, 4], "letters": ["a", "b", "c", "d"]})
        expected = dd.from_pandas(df, npartitions=2)
        ddf = beavis.create_ddf(
            [[1, "a"], [2, "b"], [3, "c"], [4, "d"]], ["nums", "letters"], npartitions=2
        )
        assert expected.npartitions == 2
        assert ddf.npartitions == 2
        beavis.assert_dd_equality(expected, ddf)


def describe_memory_by_partition():
    def it_returns_memory_by_partition():
        ddf = beavis.create_ddf(
            [[1, "a"], [2, "b"], [3, "c"], [4, "d"]], ["nums", "letters"], npartitions=2
        )
        res = beavis.memory_by_partition(ddf)
        print(res)
