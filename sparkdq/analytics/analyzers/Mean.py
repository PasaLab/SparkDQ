from pyspark.sql.functions import sum as _sum, count

from sparkdq.analytics.Analyzer import StandardScanShareableAnalyzer
from sparkdq.analytics.CommonFunctions import if_no_nulls_in, conditional_selection
from sparkdq.analytics.Preconditions import is_numeric
from sparkdq.analytics.states.MeanState import MeanState
from sparkdq.structures.Entity import Entity


class Mean(StandardScanShareableAnalyzer):

    def __init__(self, column, where=None):
        super(Mean, self).__init__(Mean.__name__, Entity.Column, column)
        self.where = where

    def __eq__(self, other):
        return (self.instance == other.instance) and (self.where == other.where)

    def __hash__(self):
        return hash((Mean.__name__, self.instance, self.where))

    def additional_preconditions(self):
        return [is_numeric(self.instance)]

    def aggregation_functions(self):
        return [_sum(conditional_selection(self.instance, self.where)).cast("double"),
                count(conditional_selection(self.instance, self.where)).cast("long")]

    def fetch_state_from_aggregation_result(self, result, offset):
        if if_no_nulls_in(result, offset, how_many=2):
            return MeanState(result[offset], result[offset+1])
        return None

    def to_json(self):
        return {
            "AnalyzerName": Mean.__name__,
            "Column": self.instance,
            "Where": self.where
        }

    @staticmethod
    def from_json(d):
        return Mean(d["Column"], d["Where"])
