from pyspark.sql.functions import sum as _sum, col, lit, count

from sparkdq.analytics.CommonFunctions import COUNT_COL, if_no_nulls_in, metric_from_value, metric_from_empty
from sparkdq.analytics.GroupingAnalyzers import ScanShareableFrequencyBasedAnalyzer
from sparkdq.analytics.metrics.DoubleMetric import DoubleMetric
from sparkdq.structures.Entity import Entity


class UniqueRatio(ScanShareableFrequencyBasedAnalyzer):

    def __init__(self, column_or_columns):
        if not isinstance(column_or_columns, list):
            column_or_columns = list(column_or_columns)
        column_or_columns = sorted(column_or_columns)
        entity = Entity.Multicolumn if len(column_or_columns) > 1 else Entity.Column
        super(UniqueRatio, self).__init__(UniqueRatio.__name__, entity, column_or_columns)

    def __eq__(self, other):
        return self.instance == other.instance

    def __hash__(self):
        return hash((UniqueRatio.__name__, ",".join(self.instance)))

    def aggregation_functions(self, num_rows):
        return [_sum(col(COUNT_COL).__eq__(lit(1)).cast("double")), count("*").cast("double")]

    def compute_metric_from_aggregation_result(self, result, offset=0):
        cls = DoubleMetric
        if if_no_nulls_in(result, offset, 2):
            unique = result[offset]
            distinct = result[offset+1]
            return metric_from_value(self, cls, unique / distinct)
        return metric_from_empty(self, cls)

    def to_json(self):
        return {
            "AnalyzerName": UniqueRatio.__name__,
            "Columns": self.instance
        }

    @staticmethod
    def from_json(d):
        return UniqueRatio(d["Columns"])
