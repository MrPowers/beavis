import pytest

import pandas as pd


def describe_assert_frame_equal():
    def it_throws_error_when_df_content_is_not_equal():
        df1 = pd.DataFrame({"col1": [1, 2], "col2": [3, 4]})
        df2 = pd.DataFrame({"col1": [5, 2], "col2": [3, 4]})
        with pytest.raises(AssertionError) as e_info:
            pd.testing.assert_frame_equal(df1, df2)

    def it_throws_error_when_index_is_not_equal():
        df1 = pd.DataFrame({"col1": [1, 2], "col2": [3, 4]}, index=["a", "b"])
        df2 = pd.DataFrame({"col1": [5, 2], "col2": [3, 4]})
        with pytest.raises(AssertionError) as e_info:
            pd.testing.assert_frame_equal(df1, df2)

    def it_throws_when_dtypes_do_not_match():
        df1 = pd.DataFrame({"col1": [1.0, 2], "col2": [3.0, 4]})
        df2 = pd.DataFrame({"col1": [1, 2], "col2": [3, 4]})
        with pytest.raises(AssertionError) as e_info:
            pd.testing.assert_frame_equal(df1, df2)


def describe_assert_series_equal():
    def it_throws_when_series_content_is_not_equal():
        df = pd.DataFrame({"col1": [1042, 2, 9, 6], "col2": [5, 2, 7, 6]})
        with pytest.raises(AssertionError) as e_info:
            pd.testing.assert_series_equal(df["col1"], df["col2"])
