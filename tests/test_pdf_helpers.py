import pytest
import beavis
import pandas as pd


def describe_create_pdf():
    def it_creates_a_pandas_dataframe():
        df = pd.DataFrame({"nums": [1, 2, 3], "letters": ["a", "b", "c"]})
        actual = beavis.create_pdf([(1, "a"), (2, "b"), (3, "c")], ["nums", "letters"])
        beavis.assert_pd_equality(df, actual)
