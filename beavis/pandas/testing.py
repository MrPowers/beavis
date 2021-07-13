from beavis.prettytable import PrettyTable
from beavis.bcolors import *
import beavis.six as six


class BeavisDataFramesNotEqualError(Exception):
    """The DataFrames are not equal"""

    pass


class BeavisIndicesNotEqualError(Exception):
    """The indices are not equal"""

    pass


class BeavisDTypesNotEqualError(Exception):
    """The indices are not equal"""

    pass


class BeavisColumnsNotEqualError(Exception):
    """The columns are not equal"""

    pass


def assert_df_equality(df1, df2, check_index=True, check_dtype=True):
    if check_index:
        assert_index_equality(df1, df2)
    if check_dtype:
        assert_dtype_equality(df1, df2)
    rows1 = df1.values.tolist()
    rows2 = df2.values.tolist()
    if rows1 != rows2:
        t = PrettyTable(["df1", "df2"])
        zipped = list(six.moves.zip_longest(rows1, rows2))
        for r1, r2 in zipped:
            if r1 == r2:
                t.add_row([blue(r1), blue(r2)])
            else:
                t.add_row([r1, r2])
        raise BeavisDataFramesNotEqualError("\n" + t.get_string())


def assert_index_equality(df1, df2):
    index1 = df1.index.tolist()
    index2 = df2.index.tolist()
    if index1 != index2:
        t = PrettyTable(["index1", "index2"])
        zipped = list(six.moves.zip_longest(index1, index2))
        for r1, r2 in zipped:
            if r1 == r2:
                t.add_row([blue(r1), blue(r2)])
            else:
                t.add_row([r1, r2])
        raise BeavisIndicesNotEqualError("\n" + t.get_string())


def assert_dtype_equality(df1, df2):
    dtypes1 = df1.dtypes.tolist()
    dtypes2 = df2.dtypes.tolist()
    if dtypes1 != dtypes2:
        t = PrettyTable(["dtypes1", "dtypes2"])
        zipped = list(six.moves.zip_longest(dtypes1, dtypes2))
        for r1, r2 in zipped:
            if r1 == r2:
                t.add_row([blue(r1), blue(r2)])
            else:
                t.add_row([r1, r2])
        raise BeavisDTypesNotEqualError("\n" + t.get_string())


def assert_column_equality(df, col_name1, col_name2):
    elements = df[[col_name1, col_name2]].values.tolist()
    colName1Elements = list(map(lambda x: x[0], elements))
    colName2Elements = list(map(lambda x: x[1], elements))
    if colName1Elements != colName2Elements:
        zipped = list(zip(colName1Elements, colName2Elements))
        t = PrettyTable([col_name1, col_name2])
        for elements in zipped:
            if elements[0] == elements[1]:
                first = bcolors.LightBlue + str(elements[0]) + bcolors.LightRed
                second = bcolors.LightBlue + str(elements[1]) + bcolors.LightRed
                t.add_row([first, second])
            else:
                t.add_row([str(elements[0]), str(elements[1])])
        raise BeavisColumnsNotEqualError("\n" + t.get_string())
