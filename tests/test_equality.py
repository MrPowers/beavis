import pytest
import beavis
import math

def describe_lists_are_equal():
    def it_returns_true_for_equivalent_lists():
        l1 = [1, 2, 3]
        l2 = [1, 2, 3]
        assert(beavis.lists_are_equal(l1, l2) == True)

    def it_returns_false_for_different_lists():
        l1 = [1, 2, 5]
        l2 = [1, 2, 3]
        assert(beavis.lists_are_equal(l1, l2) == False)

    def it_allows_for_custom_equality():
        l1 = [1.05, 1.92, 3.01]
        l2 = [1, 2, 3]
        def approx_equality(a, b):
            return math.isclose(a, b, rel_tol=0.1)
        assert(beavis.lists_are_equal(l1, l2, approx_equality) == True)

    def it_can_fail_for_custom_equality():
        l1 = [4.5, 1.92, 3.01]
        l2 = [1, 2, 3]
        def approx_equality(a, b):
            return math.isclose(a, b, rel_tol=0.1)
        assert(beavis.lists_are_equal(l1, l2, approx_equality) == False)

