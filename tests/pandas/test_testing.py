import pytest

import pandas as pd
from beavis.pandas.testing import *


def test_assert_df_equality():
    df1 = pd.DataFrame({"col1": [1, 2], "col2": [3, 4]})
    df2 = pd.DataFrame({"col1": [5, 2], "col2": [3, 4]})
    with pytest.raises(BeavisDataFramesNotEqualError) as e_info:
        assert_df_equality(df1, df2)


def test_assert_column_equality():
    df = pd.DataFrame({"col1": [1, 2, 9, 6], "col2": [5, 2, 7, 6]})
    with pytest.raises(BeavisColumnsNotEqualError) as e_info:
        assert_column_equality(df, "col1", "col2")
