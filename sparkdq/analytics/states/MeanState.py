from sparkdq.analytics.states.State import DoubleValuedState


class MeanState(DoubleValuedState):

    def __init__(self, summation, count):
        self.summation = summation
        self.count = count

    def metric_value(self):
        if self.count == 0:
            return None
        return self.summation / self.count

    # def sum(self, other):
    #     return MeanState(self.summation + other.summation, self.count + other.count)
