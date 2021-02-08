from pyspark.sql.functions import sum as _sum

from sparkdq.analytics.Analyzer import StandardScanShareableAnalyzer
from sparkdq.analytics.CommonFunctions import if_no_nulls_in, conditional_selection
from sparkdq.analytics.Preconditions import is_numeric
from sparkdq.analytics.states.SumState import SumState
from sparkdq.structures.Entity import Entity


class Sum(StandardScanShareableAnalyzer):

    def __init__(self, column, where=None):
        super(Sum, self).__init__(Sum.__name__, Entity.Column, column)
        self.where = where

    def __eq__(self, other):
        return (self.instance == other.instance) and (self.where == other.where)

    def __hash__(self):
        return hash((Sum.__name__, self.where))

    def additional_preconditions(self):
        return [is_numeric(self.instance)]

    def aggregation_functions(self):
        return [_sum(conditional_selection(self.instance, self.where))]

    def fetch_state_from_aggregation_result(self, result, offset):
        if if_no_nulls_in(result, offset):
            return SumState(result[offset])
        return None

    def to_json(self):
        return {
            "AnalyzerName": Sum.__name__,
            "Column": self.instance,
            "Where": self.where
        }

    @staticmethod
    def from_json(d):
        return Sum(d["Column"], d["Where"])
