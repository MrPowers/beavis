# beavis

Pandas and Dask test helper methods.

![cornholio](https://github.com/MrPowers/beavis/blob/main/images/cornholio.jpg)

## pandas test helpers

The built-in `pandas.testing.assert_frame_equal` method doesn't output an error message that's easy to parse, see this example.

```python
df1 = pd.DataFrame({'col1': [1, 2], 'col2': [3, 4]})
df2 = pd.DataFrame({'col1': [5, 2], 'col2': [3, 4]})
pd.testing.assert_frame_equal(df1, df2)
```

```
E   AssertionError: DataFrame.iloc[:, 0] (column name="col1") are different
E
E   DataFrame.iloc[:, 0] (column name="col1") values are different (50.0 %)
E   [index]: [0, 1]
E   [left]:  [1, 2]
E   [right]: [5, 2]
```

beavis provides a nicer error message.

```python
from beavis.pandas.testing import assert_df_equality

assert_df_equality(df1, df2)
```

![BeavisDataFramesNotEqualError](https://github.com/MrPowers/beavis/blob/main/images/beavis_dataframes_not_equal_error.png)

