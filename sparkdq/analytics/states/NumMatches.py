from sparkdq.analytics.states.State import DoubleValuedState


class NumMatches(DoubleValuedState):
    """
    Number of matches
    """
    def __init__(self, num_matches):
        self.num_matches = num_matches

    def metric_value(self):
        return self.num_matches

    # def sum(self, other):
    #     return NumMatches(self.num_matches + other.num_matches)
