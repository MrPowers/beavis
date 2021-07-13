import pytest

import pandas as pd
from beavis.pandas.testing import assert_df_equality, BeavisDataFramesNotEqualError

def test_assert_df_equality():
    df1 = pd.DataFrame({'col1': [1, 2], 'col2': [3, 4]})
    df2 = pd.DataFrame({'col1': [5, 2], 'col2': [3, 4]})
    with pytest.raises(BeavisDataFramesNotEqualError) as e_info:
        assert_df_equality(df1, df2)

