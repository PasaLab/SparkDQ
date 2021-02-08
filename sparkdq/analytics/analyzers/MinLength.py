from pyspark.sql.functions import length, min as _min

from sparkdq.analytics.Analyzer import StandardScanShareableAnalyzer
from sparkdq.analytics.CommonFunctions import if_no_nulls_in, conditional_selection
from sparkdq.analytics.Preconditions import is_string
from sparkdq.analytics.states.MinState import MinState
from sparkdq.structures.Entity import Entity


class MinLength(StandardScanShareableAnalyzer):

    def __init__(self, column, where=None):
        super(MinLength, self).__init__(MinLength.__name__, Entity, column)
        self.where = where

    def __eq__(self, other):
        return (self.instance == other.instance) and (self.where == other.where)

    def __hash__(self):
        return hash((MinLength.__name__, self.instance, self.where))

    def additional_preconditions(self):
        return [is_string(self.instance)]

    def aggregation_functions(self):
        return [_min(length(conditional_selection(self.instance, self.where))).cast("double")]

    def fetch_state_from_aggregation_result(self, result, offset):
        if if_no_nulls_in(result, offset):
            return MinState(result[offset])
        return None

    def to_json(self):
        return {
            "AnalyzerName": MinLength.__name__,
            "Column": self.instance,
            "Where": self.where
        }

    @staticmethod
    def from_json(d):
        return MinLength(d["Column"], d["Where"])
