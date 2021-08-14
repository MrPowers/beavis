import pytest

import math

import pandas as pd
import dask.dataframe as dd
import beavis


def startswith_s(df, input_col, output_col):
    df[output_col] = df[input_col].str.startswith("s")


def describe_pandas_column_testing():
    def it_shows_standard_test_with_built_in():
        df = pd.DataFrame({"col1": ["sap", "hi"], "col2": [3, 4]})
        startswith_s(df, "col1", "col1_startswith_s")
        expected = pd.Series([True, False], name="col1_startswith_s")
        pd.testing.assert_series_equal(df["col1_startswith_s"], expected)

    def it_shows_standard_test_with_beavis():
        df = pd.DataFrame(
            {"col1": ["sap", "hi"], "col2": [3, 4], "expected": [True, False]}
        )
        startswith_s(df, "col1", "col1_startswith_s")
        beavis.assert_pd_column_equality(df, "col1_startswith_s", "expected")

    def it_shows_best_formatting():
        df = beavis.create_pdf(
            [("sap", 3, True), ("hi", 4, False)], ["col1", "col2", "expected"]
        )
        startswith_s(df, "col1", "col1_startswith_s")
        beavis.assert_pd_column_equality(df, "col1_startswith_s", "expected")


def approx_equality(a, b):
    return math.isclose(a, b, rel_tol=0.1)


# def describe_pandas_dataframe_testing():
# def it_shows_custom_equality_testing():
# df1 = beavis.create_pdf([(1.0, "bye"), (2.4, "bye")], ["col1", "col2"])
# df2 = beavis.create_pdf([(1.05, "bye"), (3.9, "bye")], ["col1", "col2"])
# beavis.assert_pd_equality(df1, df2, equality_funs = {"col1": approx_equality})


# def approx_equality(a, b):
# return math.isclose(a, b, rel_tol=0.1)


# def describe_assert_pd_equality():
# def it_throws_when_df_content_is_not_equal():
# df1 = pd.DataFrame({"col1": [1, 2], "col2": [3, 4]})
# df2 = pd.DataFrame({"col1": [5, 2], "col2": [3, 4]})
# beavis.assert_pd_equality(df1, df2)

# def describe_assert_dd_equality():

# def it_throws_when_df_content_is_not_equal():
# df1 = pd.DataFrame({"col1": [1, 2], "col2": [3, 4]})
# ddf1 = dd.from_pandas(df1, npartitions=2)
# df2 = pd.DataFrame({"col1": [5, 2], "col2": [3, 4]})
# ddf2 = dd.from_pandas(df2, npartitions=2)
# beavis.assert_dd_equality(ddf1, ddf2)

# def it_throws_when_dtypes_do_not_match():
# df1 = pd.DataFrame({"col1": [1.0, 2], "col2": [3.0, 4]})
# ddf1 = dd.from_pandas(df1, npartitions=2)
# df2 = pd.DataFrame({"col1": [1, 2], "col2": [3, 4]})
# ddf2 = dd.from_pandas(df2, npartitions=2)
# beavis.assert_dd_equality(ddf1, ddf2)
