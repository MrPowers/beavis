# beavis

Pandas helper methods.

![cornholio](https://github.com/MrPowers/beavis/blob/main/images/cornholio.jpg)

See [this blog post](https://mungingdata.com/pandas/unit-testing-pandas/) for a full description on how to use Beavis to test your code.

## Intall

Install with pip: `pip install beavis`

## test helpers

These test helper methods are meant to be used in test suites.  They provide descriptive error messages to allow for a seamless development workflow.

The test helpers are inspired by [chispa](https://github.com/MrPowers/chispa) and [spark-fast-tests](https://github.com/MrPowers/spark-fast-tests), popular test helper libraries for the PySpark / Scala Spark ecosystem.

There are built-in Pandas testing methods that can also be used, but they don't provide error messages that are as easy to parse.  The following sections compare the built-in Pandas output and what's output by Beavis, so you can choose for yourself.

### Column comparisons

The built-in `assert_series_equal` method does not make it easy to decipher the rows that are equal and the rows that are different, so quickly fixing your tests and maintaining flow is hard.

Here's the built-in error message when comparing series that are not equal.

```python
df = pd.DataFrame({"col1": [1042, 2, 9, 6], "col2": [5, 2, 7, 6]})
pd.testing.assert_series_equal(df["col1"], df["col2"])
```

```
>   ???
E   AssertionError: Series are different
E
E   Series values are different (50.0 %)
E   [index]: [0, 1, 2, 3]
E   [left]:  [1042, 2, 9, 6]
E   [right]: [5, 2, 7, 6]
```

Here's the beavis error message that aligns rows and highlights the mismatches in red.

```python
import beavis

beavis.assert_pd_column_equality(df, "col1", "col2")
```

![BeavisColumnsNotEqualError](https://github.com/MrPowers/beavis/blob/main/images/beavis_columns_not_equal_error.png)

### DataFrame comparisons

The built-in `pandas.testing.assert_frame_equal` method doesn't output an error message that's easy to understand, see this example.

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
beavis.assert_pd_equality(df1, df2)
```

![BeavisDataFramesNotEqualError](https://github.com/MrPowers/beavis/blob/main/images/assert_pd_equality_error.png)

DataFrame comparison options:

* `check_index` (default `True`)
* `check_dtype` (default `True`)

## Development

Install Poetry and run `poetry install` to create a virtual environment with all the Beavis dependencies on your machine.

Other useful commands:

* `poetry run pytest tests` runs the test suite
* `poetry run black .` to format the code
* `poetry build` packages the library in a wheel file
* `poetry publish` releases the library in PyPi (need correct credentials)

