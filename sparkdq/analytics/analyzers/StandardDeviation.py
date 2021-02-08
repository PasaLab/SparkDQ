from sparkdq.analytics.Analyzer import StandardScanShareableAnalyzer
from sparkdq.analytics.catalyst.DQFunctions import stateful_stddev
from sparkdq.analytics.CommonFunctions import if_no_nulls_in, conditional_selection
from sparkdq.analytics.Preconditions import is_numeric
from sparkdq.analytics.states.StandardDeviationState import StandardDeviationState
from sparkdq.structures.Entity import Entity


class StandardDeviation(StandardScanShareableAnalyzer):

    def __init__(self, column, where=None):
        super(StandardDeviation, self).__init__(StandardDeviation.__name__, Entity.Column, column)
        self.where = where

    def __eq__(self, other):
        return (self.instance == other.instance) and (self.where == other.where)

    def __hash__(self):
        return hash((StandardDeviation.__name__, self.instance, self.where))

    def additional_preconditions(self):
        return [is_numeric(self.instance)]

    def aggregation_functions(self):
        return [stateful_stddev(conditional_selection(self.instance, self.where))]

    def fetch_state_from_aggregation_result(self, result, offset):
        if if_no_nulls_in(result, offset):
            row = result[offset]
            if row[0] == 0:
                return None
            return StandardDeviationState(row[0], row[1], row[2])
        return None

    def to_json(self):
        return {
            "AnalyzerName": StandardDeviation.__name__,
            "Column": self.instance,
            "Where": self.where
        }

    @staticmethod
    def from_json(d):
        return StandardDeviation(d["Column"], d["Where"])
