from pyspark.sql.functions import col, when, regexp_extract, lit, sum as _sum

from sparkdq.analytics.Analyzer import StandardScanShareableAnalyzer
from sparkdq.analytics.CommonFunctions import conditional_selection, conditional_count, if_no_nulls_in
from sparkdq.analytics.states.NumMatchesAndRows import NumMatchesAndRows
from sparkdq.structures.Entity import Entity


class PatternMatch(StandardScanShareableAnalyzer):

    def __init__(self, column, pattern, where=None):
        super(PatternMatch, self).__init__(PatternMatch.__name__, Entity.Column, column)
        self.pattern = pattern
        self.where = where

    def __eq__(self, other):
        return (self.instance == other.instance) and (self.pattern == other.pattern) and (self.where == other.where)

    def __hash__(self):
        return hash((PatternMatch.__name__, self.pattern, self.instance, self.where))

    def additional_preconditions(self):
        return []

    def aggregation_functions(self):
        expression = when(regexp_extract(self.instance, self.pattern, 0) != lit(""), 1).otherwise(0)
        return [_sum(conditional_selection(expression, self.where).cast("integer")), conditional_count(self.where)]

    def fetch_state_from_aggregation_result(self, result, offset):
        if if_no_nulls_in(result, offset, how_many=2):
            return NumMatchesAndRows(result[offset], result[offset+1])
        return None

    def to_json(self):
        return {
            "AnalyzerName": PatternMatch.__name__,
            "Column": self.instance,
            "Where": self.where,
            "Pattern": self.pattern
        }

    @staticmethod
    def from_json(d):
        return PatternMatch(d["Column"], d["Where"], d["Pattern"])
