import pytest

import pandas as pd
import dask.dataframe as dd
import beavis


def describe_assert_pd_equality():
    # Vanilla equality
    def it_throws_when_df_content_is_not_equal():
        df1 = pd.DataFrame({"col1": [1, 2], "col2": [3, 4]})
        df2 = pd.DataFrame({"col1": [5, 2], "col2": [3, 4]})
        with pytest.raises(beavis.BeavisDataFramesNotEqualError) as e_info:
            beavis.assert_pd_equality(df1, df2)

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


def describe_assert_pd_column_equality():
    def it_throws_when_df_columns_are_not_equal():
        df = pd.DataFrame({"col1": [1, 2, 9, 6], "col2": [5, 2, 7, 6]})
        with pytest.raises(beavis.BeavisColumnsNotEqualError) as e_info:
            beavis.assert_pd_column_equality(df, "col1", "col2")

def describe_assert_df_equality():
    # Vanilla equality
    def it_throws_when_df_content_is_not_equal():
        df1 = pd.DataFrame({"col1": [1, 2], "col2": [3, 4]})
        ddf1 = dd.from_pandas(df1, npartitions=2)
        df2 = pd.DataFrame({"col1": [5, 2], "col2": [3, 4]})
        ddf2 = dd.from_pandas(df2, npartitions=2)
        with pytest.raises(beavis.BeavisDataFramesNotEqualError) as e_info:
            beavis.assert_dd_equality(ddf1, ddf2)

    def it_does_not_throw_when_dfs_are_equal():
        df1 = pd.DataFrame({"col1": [1, 2], "col2": [3, 4]})
        ddf1 = dd.from_pandas(df1, npartitions=2)
        df2 = pd.DataFrame({"col1": [1, 2], "col2": [3, 4]})
        ddf2 = dd.from_pandas(df2, npartitions=2)
        beavis.assert_dd_equality(ddf1, ddf2)
