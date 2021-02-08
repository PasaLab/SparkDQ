from pyspark.sql.functions import max as _max

from sparkdq.analytics.Analyzer import StandardScanShareableAnalyzer
from sparkdq.analytics.CommonFunctions import conditional_selection, if_no_nulls_in
from sparkdq.analytics.Preconditions import is_numeric
from sparkdq.analytics.states.MaxState import MaxState
from sparkdq.structures.Entity import Entity


class Maximum(StandardScanShareableAnalyzer):
    
    def __init__(self, column, where=None):
        super(Maximum, self).__init__(Maximum.__name__, Entity.Column, column)
        self.where = where

    def __eq__(self, other):
        return (self.instance == other.instance) and (self.where == other.where)

    def __hash__(self):
        return hash((Maximum.__name__, self.instance, self.where))

    def additional_preconditions(self):
        return [is_numeric(self.instance)]

    def aggregation_functions(self):
        return [_max(conditional_selection(self.instance, self.where).cast("double"))]

    def fetch_state_from_aggregation_result(self, result, offset):
        if if_no_nulls_in(result, offset):
            return MaxState(result[offset])
        return None

    def to_json(self):
        return {
            "AnalyzerName": Maximum.__name__,
            "Column": self.instance,
            "Where": self.where
        }

    @staticmethod
    def from_json(d):
        return Maximum(d["Column"], d["Where"])
