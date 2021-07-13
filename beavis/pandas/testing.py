from beavis.prettytable import PrettyTable
from beavis.bcolors import *
import beavis.six as six


class BeavisDataFramesNotEqualError(Exception):
    """The DataFrames are not equal"""

    pass


class BeavisColumnsNotEqualError(Exception):
    """The columns are not equal"""

    pass


def assert_df_equality(df1, df2):
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
