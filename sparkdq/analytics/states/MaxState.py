from sparkdq.analytics.states.State import DoubleValuedState


class MaxState(DoubleValuedState):

    def __init__(self, max_value):
        self.max_value = max_value

    def metric_value(self):
        return self.max_value

    # def sum(self, other):
    #     return MaxState(max(self.max_value, other.max_value))
