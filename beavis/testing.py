from beavis.prettytable import PrettyTable
from beavis.bcolors import *
import beavis.six as six
import beavis


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


def assert_pd_equality(
    df1, df2, check_index=True, check_dtype=True, equality_funs=None
):
    if check_index:
        assert_pd_index_equality(df1, df2)
    if check_dtype:
        assert_pd_dtype_equality(df1, df2)
    rows1 = df1.values.tolist()
    rows2 = df2.values.tolist()
    if equality_funs is None:
        equality_funs = {}
    col_names = df1.columns.values.tolist()
    col_equalities = list(
        map(
            lambda x: equality_funs.get(x, beavis.equality.default_equality),
            col_names,
        )
    )
    t = PrettyTable(["df1", "df2"])
    zipped = list(six.moves.zip_longest(rows1, rows2))
    all_equal = True
    for r1, r2 in zipped:
        z2 = list(zip(r1, r2, col_equalities))
        colored_r1 = []
        colored_r2 = []
        for e1, e2, fun in z2:
            if fun(e1, e2):
                colored_r1.append(blue(e1))
                colored_r2.append(blue(e2))
            else:
                all_equal = False
                colored_r1.append(str(e1))
                colored_r2.append(str(e2))
        t.add_row([", ".join(colored_r1), ", ".join(colored_r2)])
    if not all_equal:
        raise BeavisDataFramesNotEqualError("\n" + t.get_string())


def assert_pd_index_equality(df1, df2):
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


def assert_pd_dtype_equality(df1, df2):
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


def assert_pd_column_equality(df, col_name1, col_name2, equality_fun=None):
    elements = df[[col_name1, col_name2]].values.tolist()
    colName1Elements = list(map(lambda x: x[0], elements))
    colName2Elements = list(map(lambda x: x[1], elements))
    all_equal = None
    if equality_fun is None:
        all_equal = colName1Elements == colName2Elements
    else:
        all_equal = beavis.equality.lists_are_equal(
            colName1Elements, colName2Elements, equality_fun
        )
    if not all_equal:
        zipped = list(zip(colName1Elements, colName2Elements))
        t = PrettyTable([col_name1, col_name2])
        for elements in zipped:
            if (
                elements[0] == elements[1]
                if equality_fun is None
                else equality_fun(elements[0], elements[1])
            ):
                first = bcolors.LightBlue + str(elements[0]) + bcolors.LightRed
                second = bcolors.LightBlue + str(elements[1]) + bcolors.LightRed
                t.add_row([first, second])
            else:
                t.add_row([str(elements[0]), str(elements[1])])
        raise BeavisColumnsNotEqualError("\n" + t.get_string())


def assert_dd_equality(df1, df2, check_index=True, check_dtype=True):
    assert_pd_equality(df1.compute(), df2.compute())


def assert_dd_column_equality(df, col_name1, col_name2):
    assert_pd_column_equality(df.compute(), col_name1, col_name2)
