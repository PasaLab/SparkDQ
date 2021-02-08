from sparkdq.analytics.states.State import DoubleValuedState


class ModeState(DoubleValuedState):

    def __init__(self, mode_value):
        self.mode_value = mode_value

    def metric_value(self):
        return self.mode_value

    # def sum(self, other):
    #     return ModeState(max(self.mode_value, other.mode_value))
