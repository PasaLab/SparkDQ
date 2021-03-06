from pyspark.sql.functions import length, max as _max

from sparkdq.analytics.Analyzer import StandardScanShareableAnalyzer
from sparkdq.analytics.CommonFunctions import if_no_nulls_in, conditional_selection
from sparkdq.analytics.Preconditions import is_string
from sparkdq.analytics.states.MaxState import MaxState
from sparkdq.structures.Entity import Entity


class MaxLength(StandardScanShareableAnalyzer):
    
    def __init__(self, column, where=None):
        super(MaxLength, self).__init__(MaxLength.__name__, Entity, column)
        self.where = where

    def __eq__(self, other):
        return (self.instance == other.instance) and (self.where == other.where)

    def __hash__(self):
        return hash((MaxLength.__name__, self.instance, self.where))

    def additional_preconditions(self):
        return [is_string(self.instance)]

    def aggregation_functions(self):
        return [_max(length(conditional_selection(self.instance, self.where))).cast("double")]

    def fetch_state_from_aggregation_result(self, result, offset):
        if if_no_nulls_in(result, offset):
            return MaxState(result[offset])
        return None

    def to_json(self):
        return {
            "AnalyzerName": MaxLength.__name__,
            "Column": self.instance,
            "Where": self.where
        }

    @staticmethod
    def from_json(d):
        return MaxLength(d["Column"], d["Where"])
