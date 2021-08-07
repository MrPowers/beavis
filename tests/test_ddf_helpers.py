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
        beavis.print_dtypes(ddf)
