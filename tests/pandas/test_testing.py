import pytest

import pandas as pd
from beavis.pandas.testing import *


def describe_assert_df_equality():
    def it_throws_when_df_content_is_not_equal():
        df1 = pd.DataFrame({"col1": [1, 2], "col2": [3, 4]})
        df2 = pd.DataFrame({"col1": [5, 2], "col2": [3, 4]})
        with pytest.raises(BeavisDataFramesNotEqualError) as e_info:
            assert_df_equality(df1, df2)

    def it_throws_when_index_does_not_match():
        df1 = pd.DataFrame({"col1": [1, 2], "col2": [3, 4]}, index=["a", 1])
        df2 = pd.DataFrame({"col1": [1, 2], "col2": [3, 4]})
        with pytest.raises(BeavisIndicesNotEqualError) as e_info:
            assert_df_equality(df1, df2)

    def it_does_not_throw_throw_if_index_is_ignored():
        df1 = pd.DataFrame({"col1": [1, 2], "col2": [3, 4]}, index=["a", 1])
        df2 = pd.DataFrame({"col1": [1, 2], "col2": [3, 4]})
        assert_df_equality(df1, df2, check_index=False)


def describe_assert_column_equality():
    def it_throws_when_df_columns_are_not_equal():
        df = pd.DataFrame({"col1": [1, 2, 9, 6], "col2": [5, 2, 7, 6]})
        with pytest.raises(BeavisColumnsNotEqualError) as e_info:
            assert_column_equality(df, "col1", "col2")
