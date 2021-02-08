from sparkdq.analytics.states.State import DoubleValuedState


class NumMatchesAndRows(DoubleValuedState):
    """
    Number of matches and total rows for ratio
    """
    def __init__(self, num_matches, num_rows):
        self.num_matches = num_matches
        self.num_rows = num_rows

    def metric_value(self):
        if self.num_rows == 0:
            return 0
        return self.num_matches / self.num_rows

    # def sum(self, other):
    #     return NumMatchesAndRows(self.num_matches + other.num_matches, self.num_rows + other.num_rows)
