from sparkdq.analytics.states.State import DoubleValuedState


class SumState(DoubleValuedState):

    def __init__(self, summation):
        self.summation = summation

    def metric_value(self):
        return self.summation

    # def sum(self, other):
    #     # if not isinstance(other, SumState):
    #     #     raise Exception("Inconsistent states, self is {} and the other is {}.".format("SumState", type(other)))
    #     return SumState(self.summation + other.summation)
