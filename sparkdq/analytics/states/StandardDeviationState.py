import math

from sparkdq.analytics.states.State import DoubleValuedState


class StandardDeviationState(DoubleValuedState):

    def __init__(self, n, avg, m2):
        self.n = n
        self.avg = avg
        self.m2 = m2

    def metric_value(self):
        return math.sqrt(self.m2 / self.n)

    # def sum(self, other):
    #     new_n = self.n + other.n
    #     delta = other.avg - self.avg
    #     delta_n = 0.0 if new_n == 0.0 else delta / new_n
    #     return StandardDeviationState(new_n, self.avg + delta_n * other.n,
    #                                   self.m2 + other.m2 + delta * delta_n * self.n * other.n)
