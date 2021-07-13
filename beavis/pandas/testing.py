from beavis.prettytable import PrettyTable
from beavis.bcolors import *
import beavis.six as six

class BeavisDataFramesNotEqualError(Exception):
   """The DataFrames are not equal"""
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
