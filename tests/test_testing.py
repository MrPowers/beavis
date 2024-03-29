import pytest

import math

import pandas as pd
import beavis


def approx_equality(a, b):
    return math.isclose(a, b, rel_tol=0.1)


def describe_assert_pd_equality():
    # Vanilla equality
    def it_throws_when_df_content_is_not_equal():
        df1 = pd.DataFrame({"col1": [1, 2], "col2": [3, 4]})
        df2 = pd.DataFrame({"col1": [5, 2], "col2": [3, 4]})
        with pytest.raises(beavis.BeavisDataFramesNotEqualError) as e_info:
            beavis.assert_pd_equality(df1, df2)

    # Custom equality
    def it_allows_for_custom_equality_matching():
        def approx_equality(a, b):
            return math.isclose(a, b, rel_tol=0.1)

        df1 = pd.DataFrame({"col1": [1, 2], "col2": [3.05, 3.99]})
        df2 = pd.DataFrame({"col1": [1, 2], "col2": [3, 4]})
        beavis.assert_pd_equality(
            df1, df2, check_dtype=False, equality_funs={"col2": approx_equality}
        )

    def it_can_throw_for_custom_equality_matching():
        def approx_equality(a, b):
            return math.isclose(a, b, rel_tol=0.1)

        df1 = pd.DataFrame({"col1": [1, 2], "col2": [3.8, 3.99]})
        df2 = pd.DataFrame({"col1": [1, 2], "col2": [3, 4]})
        with pytest.raises(beavis.BeavisDataFramesNotEqualError) as e_info:
            beavis.assert_pd_equality(
                df1, df2, check_dtype=False, equality_funs={"col2": approx_equality}
            )

    # INDEX
    def it_throws_when_index_does_not_match():
        df1 = pd.DataFrame({"col1": [1, 2], "col2": [3, 4]}, index=["a", 1])
        df2 = pd.DataFrame({"col1": [1, 2], "col2": [3, 4]})
        with pytest.raises(beavis.BeavisIndicesNotEqualError) as e_info:
            beavis.assert_pd_equality(df1, df2)

    def it_does_not_throw_throw_if_index_is_ignored():
        df1 = pd.DataFrame({"col1": [1, 2], "col2": [3, 4]}, index=["a", 1])
        df2 = pd.DataFrame({"col1": [1, 2], "col2": [3, 4]})
        beavis.assert_pd_equality(df1, df2, check_index=False)

    # DTYPES
    def it_throws_when_dtypes_do_not_match():
        df1 = pd.DataFrame({"col1": [1.0, 2], "col2": [3.0, 4]})
        df2 = pd.DataFrame({"col1": [1, 2], "col2": [3, 4]})
        with pytest.raises(beavis.BeavisDTypesNotEqualError) as e_info:
            beavis.assert_pd_equality(df1, df2)

    def it_does_not_throw_if_dtype_is_ignored():
        df1 = pd.DataFrame({"col1": [1.0, 2], "col2": [3.0, 4]})
        df2 = pd.DataFrame({"col1": [1, 2], "col2": [3, 4]})
        beavis.assert_pd_equality(df1, df2, check_dtype=False)


def describe_assert_approx_pd_equality():
    def it_allows_for_approx_comparisons():
        df1 = pd.DataFrame({"col1": ["hi", "aa"], "col2": [3.05, 3.99]})
        df2 = pd.DataFrame({"col1": ["hi", "aa"], "col2": [3, 4]})
        beavis.assert_approx_pd_equality(df1, df2, 0.1, check_dtype=False)


def describe_assert_pd_column_equality():
    def it_throws_when_df_columns_are_not_equal():
        df = pd.DataFrame({"col1": [1042, 2, 9, 6], "col2": [5, 2, 7, 6]})
        with pytest.raises(beavis.BeavisColumnsNotEqualError) as e_info:
            beavis.assert_pd_column_equality(df, "col1", "col2")

    # Custom equality
    def it_support_custom_equality_matches():
        df = pd.DataFrame({"col1": [1.05, 1.92, 3.01], "col2": [1, 2, 3]})

        def approx_equality(a, b):
            return math.isclose(a, b, rel_tol=0.1)

        beavis.assert_pd_column_equality(
            df, "col1", "col2", equality_fun=approx_equality
        )

    def it_support_custom_equality_failures():
        df = pd.DataFrame({"col1": [4.5, 1.92, 3.01], "col2": [1, 2, 3]})

        def approx_equality(a, b):
            return math.isclose(a, b, rel_tol=0.1)

        with pytest.raises(beavis.BeavisColumnsNotEqualError) as e_info:
            beavis.assert_pd_column_equality(
                df, "col1", "col2", equality_fun=approx_equality
            )

