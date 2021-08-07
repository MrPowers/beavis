import math


def default_equality(x, y):
    return x == y


def lists_are_equal(list1, list2, equality_fun=None):
    if equality_fun is None:
        return list1 == list2
    else:
        zipped = list(zip(list1, list2))
        return all(map(lambda x, y: equality_fun(x, y), list1, list2))


# def rows_are_equal(row1, row2, equality_funs):
