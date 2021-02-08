from pyspark.sql.functions import sum as _sum, expr

from sparkdq.analytics.Analyzer import StandardScanShareableAnalyzer
from sparkdq.analytics.CommonFunctions import conditional_selection, conditional_count, if_no_nulls_in
from sparkdq.analytics.states.NumMatchesAndRows import NumMatchesAndRows
from sparkdq.structures.Entity import Entity


class Compliance(StandardScanShareableAnalyzer):

    def __init__(self, predicate, where=None):
        """
        Note:
        1. here instance is the predicate
        2. entity is uncertain, but mostly Column, so uniformly regarded as Column
        :param predicate: str, predicate expression
        :param where:
        """
        super(Compliance, self).__init__(Compliance.__name__, Entity.Column, predicate)
        self.where = where

    def __eq__(self, other):
        return (self.where == other.where) and (self.instance == other.instance)

    def __hash__(self):
        return hash((Compliance.__name__, self.where, self.instance))

    def additional_preconditions(self):
        return []

    def preconditions(self):
        """
        Because instance is not column or columns, no preconditions for Compliance.
        :return:
        """
        return []

    def aggregation_functions(self):
        return [_sum(conditional_selection(expr(self.instance), self.where).cast("integer")),
                conditional_count(self.where)]

    def fetch_state_from_aggregation_result(self, result, offset):
        if if_no_nulls_in(result, offset, how_many=2):
            return NumMatchesAndRows(result[offset], result[offset+1])
        return None

    def to_json(self):
        return {
            "AnalyzerName": Compliance.__name__,
            "Where": self.where,
            "Predicate": self.instance
        }

    @staticmethod
    def from_json(d):
        return Compliance(d["Predicate"], d["Where"])
