#!/usr/bin/env python
"""
math

simple math function examples for use in tests

"""
import math


def square(x):
    return x*x


class Power(object):

    def __init__(self, pow=1):
        self.power = pow

    def __call__(self, x):
        return math.pow(x, self.power)


def double(x):
    return 2*x


def printer(x):
    print("printer({})".format(x))


def even(x):
    if x % 2:
        return True
    return False
