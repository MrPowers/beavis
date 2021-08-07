# import pytest

# import math

# import pandas as pd
# import dask.dataframe as dd
# import beavis


# def approx_equality(a, b):
# return math.isclose(a, b, rel_tol=0.1)


# def describe_assert_pd_equality():
# def it_throws_when_df_content_is_not_equal():
# df1 = pd.DataFrame({"col1": [1, 2], "col2": [3, 4]})
# df2 = pd.DataFrame({"col1": [5, 2], "col2": [3, 4]})
# beavis.assert_pd_equality(df1, df2)

# def describe_assert_dd_equality():

# def it_throws_when_df_content_is_not_equal():
# df1 = pd.DataFrame({"col1": [1, 2], "col2": [3, 4]})
# ddf1 = dd.from_pandas(df1, npartitions=2)
# df2 = pd.DataFrame({"col1": [5, 2], "col2": [3, 4]})
# ddf2 = dd.from_pandas(df2, npartitions=2)
# beavis.assert_dd_equality(ddf1, ddf2)

# def it_throws_when_dtypes_do_not_match():
# df1 = pd.DataFrame({"col1": [1.0, 2], "col2": [3.0, 4]})
# ddf1 = dd.from_pandas(df1, npartitions=2)
# df2 = pd.DataFrame({"col1": [1, 2], "col2": [3, 4]})
# ddf2 = dd.from_pandas(df2, npartitions=2)
# beavis.assert_dd_equality(ddf1, ddf2)
