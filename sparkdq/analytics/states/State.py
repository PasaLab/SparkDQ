"""
  All intermediate state classes for analyzers
"""

from abc import abstractmethod


class State:
    pass

    # @abstractmethod
    # def sum(self, other):
    #     pass


class DoubleValuedState(State):
    @abstractmethod
    def metric_value(self):
        pass


