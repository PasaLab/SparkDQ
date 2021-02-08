from pyspark.sql.functions import sum as _sum, col, lit

from sparkdq.analytics.CommonFunctions import COUNT_COL
from sparkdq.analytics.GroupingAnalyzers import ScanShareableFrequencyBasedAnalyzer
from sparkdq.structures.Entity import Entity


class Distinctness(ScanShareableFrequencyBasedAnalyzer):
    """
    Distinctness is the fraction of distinct values of a column(s), indicating how many different values there are
    """
    def __init__(self, column_or_columns):
        if not isinstance(column_or_columns, list):
            column_or_columns = list(column_or_columns)
        column_or_columns = sorted(column_or_columns)
        entity = Entity.Multicolumn if len(column_or_columns) > 1 else Entity.Column
        super(Distinctness, self).__init__(Distinctness.__name__, entity, column_or_columns)

    def __eq__(self, other):
        return self.instance == other.instance

    def __hash__(self):
        return hash((Distinctness.__name__, ",".join(self.instance)))

    def aggregation_functions(self, num_rows):
        return [_sum(col(COUNT_COL).__ge__(lit(1)).cast("double")) / num_rows]

    def to_json(self):
        return {
            "AnalyzerName": Distinctness.__name__,
            "Columns": self.instance
        }

    @staticmethod
    def from_json(d):
        return Distinctness(d["Columns"])
