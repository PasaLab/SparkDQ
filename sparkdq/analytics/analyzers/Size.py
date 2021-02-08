from sparkdq.analytics.Analyzer import StandardScanShareableAnalyzer
from sparkdq.analytics.CommonFunctions import conditional_count, if_no_nulls_in
from sparkdq.analytics.states.NumMatches import NumMatches
from sparkdq.structures.Entity import Entity


class Size(StandardScanShareableAnalyzer):

    def __init__(self, where=None):
        super(Size, self).__init__(Size.__name__, Entity.DataSet, "*")
        self.where = where

    def __eq__(self, other):
        return self.where == other.where

    def __hash__(self):
        return hash((Size.__name__, self.where))

    def preconditions(self):
        return []

    def additional_preconditions(self):
        return []

    def aggregation_functions(self):
        return [conditional_count(self.where)]

    def fetch_state_from_aggregation_result(self, result, offset):
        if if_no_nulls_in(result, offset):
            return NumMatches(result[offset])
        return None

    def to_json(self):
        return {
            "AnalyzerName": Size.__name__,
            "Where": self.where
        }

    @staticmethod
    def from_json(d):
        return Size(d["Where"])
