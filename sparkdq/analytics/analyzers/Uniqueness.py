from pyspark.sql.functions import col, lit, sum as _sum

from sparkdq.analytics.CommonFunctions import COUNT_COL
from sparkdq.analytics.GroupingAnalyzers import ScanShareableFrequencyBasedAnalyzer
from sparkdq.structures.Entity import Entity


class Uniqueness(ScanShareableFrequencyBasedAnalyzer):
    """
    Uniqueness is the fraction of unique values of a column(s), i.e., values that occur exactly once.
    """
    def __init__(self, column_or_columns):
        if not isinstance(column_or_columns, list):
            column_or_columns = list(column_or_columns)
        column_or_columns = sorted(column_or_columns)
        entity = Entity.Multicolumn if len(column_or_columns) > 1 else Entity.Column
        super(Uniqueness, self).__init__(Uniqueness.__name__, entity, column_or_columns)

    def __eq__(self, other):
        return self.instance == other.instance

    def __hash__(self):
        return hash((Uniqueness.__name__, ",".join(self.instance)))

    def aggregation_functions(self, num_rows):
        return [_sum(col(COUNT_COL).__eq__(lit(1)).cast("double")) / num_rows]

    def to_json(self):
        return {
            "AnalyzerName": Uniqueness.__name__,
            "Columns": self.instance
        }

    @staticmethod
    def from_json(d):
        return Uniqueness(d["Columns"])
