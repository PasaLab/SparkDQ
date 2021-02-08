from pyspark.sql.functions import min as _min

from sparkdq.analytics.Analyzer import StandardScanShareableAnalyzer
from sparkdq.analytics.CommonFunctions import if_no_nulls_in, conditional_selection
from sparkdq.analytics.Preconditions import is_numeric
from sparkdq.analytics.states.MinState import MinState
from sparkdq.structures.Entity import Entity


class Minimum(StandardScanShareableAnalyzer):

    def __init__(self, column, where=None):
        super(Minimum, self).__init__(Minimum.__name__, Entity.Column, column)
        self.where = where

    def __eq__(self, other):
        return (self.instance == other.instance) and (self.where == other.where)

    def __hash__(self):
        return hash((Minimum.__name__, self.instance, self.where))

    def additional_preconditions(self):
        return [is_numeric(self.instance)]

    def aggregation_functions(self):
        return [_min(conditional_selection(self.instance, self.where).cast("double"))]

    def fetch_state_from_aggregation_result(self, result, offset):
        if if_no_nulls_in(result, offset):
            return MinState(result[offset])
        return None

    def to_json(self):
        return {
            "AnalyzerName": Minimum.__name__,
            "Column": self.instance,
            "Where": self.where
        }

    @staticmethod
    def from_json(d):
        return Minimum(d["Column"], d["Where"])
