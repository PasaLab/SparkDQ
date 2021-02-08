from sparkdq.analytics.Analyzer import StandardScanShareableAnalyzer
from sparkdq.analytics.catalyst.DQFunctions import stateful_approx_count_distinct
from sparkdq.analytics.CommonFunctions import conditional_selection, if_no_nulls_in
from sparkdq.analytics.states.ApproxCountDistinctState import ApproxCountDistinctState
from sparkdq.structures.Entity import Entity


class ApproxCountDistinct(StandardScanShareableAnalyzer):
    """
    Calculating ApproxCountDistinct using HyperLogLog algorithm.
    """
    def __init__(self, column, where=None):
        super(ApproxCountDistinct, self).__init__(ApproxCountDistinct.__name__, Entity.Column, column)
        self.where = where

    def __eq__(self, other):
        return (self.instance == other.instance) and (self.where == other.where)

    def __hash__(self):
        return hash((ApproxCountDistinct.__name__, self.instance, self.where))

    def additional_preconditions(self):
        return []

    def aggregation_functions(self):
        return [stateful_approx_count_distinct(conditional_selection(self.instance, self.where))]

    def fetch_state_from_aggregation_result(self, result, offset):
        if if_no_nulls_in(result, offset):
            return ApproxCountDistinctState.words_from_bytes(result[offset])
        return None

    def to_json(self):
        return {
            "AnalyzerName": ApproxCountDistinct.__name__,
            "Column": self.instance,
            "Where": self.where
        }

    @staticmethod
    def from_json(d):
        return ApproxCountDistinct(d["Column"], d["Where"])
