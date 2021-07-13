import pytest

import pandas as pd

def test_assert_frame_equal():
    df1 = pd.DataFrame({'col1': [1, 2], 'col2': [3, 4]})
    df2 = pd.DataFrame({'col1': [5, 2], 'col2': [3, 4]})
    with pytest.raises(AssertionError) as e_info:
        pd.testing.assert_frame_equal(df1, df2)

