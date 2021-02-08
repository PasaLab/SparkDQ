from functools import reduce

from pyspark.sql.functions import sum as _sum, col

from sparkdq.analytics.Analyzer import StandardScanShareableAnalyzer
from sparkdq.analytics.CommonFunctions import conditional_selection, conditional_count, if_no_nulls_in
from sparkdq.analytics.states.NumMatchesAndRows import NumMatchesAndRows
from sparkdq.structures.Entity import Entity


class Completeness(StandardScanShareableAnalyzer):

    def __init__(self, column_or_columns, where=None):
        """
        :param column_or_columns: str or list of str
        :param where: str, default None
        """
        if not isinstance(column_or_columns, list):
            column_or_columns = [column_or_columns]
        column_or_columns = sorted(column_or_columns)
        entity = Entity.Multicolumn if len(column_or_columns) > 1 else Entity.Column
        super(Completeness, self).__init__(Completeness.__name__, entity, column_or_columns)
        self.where = where

    def __eq__(self, other):
        return (self.instance == other.instance) and (self.where == other.where)

    def __hash__(self):
        return hash((Completeness.__name__, ",".join(self.instance), self.where))

    def additional_preconditions(self):
        return []

    def aggregation_functions(self):
        selection = reduce(lambda x, y: x & y, [col(c).isNotNull() for c in self.instance])
        return [_sum(conditional_selection(selection, self.where).cast("integer")), conditional_count(self.where)]

    def fetch_state_from_aggregation_result(self, result, offset):
        if if_no_nulls_in(result, offset, how_many=2):
            return NumMatchesAndRows(result[offset], result[offset+1])
        return None

    def to_json(self):
        return {
            "AnalyzerName": Completeness.__name__,
            "Column": self.instance,
            "Where": self.where
        }

    @staticmethod
    def from_json(d):
        return Completeness(d["Column"], d["Where"])
