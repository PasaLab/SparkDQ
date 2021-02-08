from sparkdq.analytics.states.State import DoubleValuedState


class MinState(DoubleValuedState):

    def __init__(self, min_value):
        self.min_value = min_value

    def metric_value(self):
        return self.min_value

    # def sum(self, other):
    #     return MinState(min(self.min_value, other.min_value))
