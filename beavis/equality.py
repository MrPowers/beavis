import math

def lists_are_equal(list1, list2, equality_fun=None):
    if equality_fun is None:
        return list1 == list2
    else:
        zipped = list(zip(list1, list2))
        return all(map(lambda x, y: equality_fun(x, y), list1, list2))
