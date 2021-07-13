import pytest

import pandas as pd

def test_assert_frame_equal():
    df1 = pd.DataFrame({'col1': [1, 2], 'col2': [3, 4]})
    df2 = pd.DataFrame({'col1': [5, 2], 'col2': [3, 4]})
    with pytest.raises(AssertionError) as e_info:
        pd.testing.assert_frame_equal(df1, df2)


def test_assert_series_equal():
    df = pd.DataFrame({'col1': [1, 2, 9, 6], 'col2': [5, 2, 7, 6]})
    with pytest.raises(AssertionError) as e_info:
        pd.testing.assert_series_equal(df["col1"], df["col2"])

