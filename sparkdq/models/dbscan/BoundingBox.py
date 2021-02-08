import sys

import numpy as np


class BoundingBox:

    def __init__(self, lower=None, upper=None, k=None):
        """

        :param lower:
        :type lower:
        :param upper:
        :param k:
        """
        if lower is not None:
            self.lower = np.array(lower)
            self.upper = np.array(upper) if upper is not None else self.lower
        elif k is not None:
            self.lower = np.full(k, sys.float_info.max)
            self.upper = np.full(k, sys.float_info.min)
        else:
            self.lower = None
            self.upper = None

    def union(self, other):
        lower = np.minimum(self.lower, other.lower)
        upper = np.maximum(self.upper, other.upper)
        return BoundingBox(lower, upper)

    def split(self, axis, value):
        left = BoundingBox(lower=np.copy(self.lower), upper=np.copy(self.upper))
        left.lower[axis] = value
        right = BoundingBox(lower=np.copy(self.lower), upper=np.copy(self.upper))
        right.upper[axis] = value
        return left, right

    def expand(self, eps=0):
        return BoundingBox(self.lower - eps, self.upper + eps)

    def contains(self, vector):
        return np.all(self.lower <= vector) and np.all(self.upper >= vector)
