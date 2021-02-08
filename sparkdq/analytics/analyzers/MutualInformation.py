import math

from pyspark.sql.functions import sum as _sum, udf, col

from sparkdq.analytics.CommonFunctions import metric_from_empty, metric_from_value, metric_from_failure, COUNT_COL
from sparkdq.analytics.GroupingAnalyzers import FrequencyBasedAnalyzer
from sparkdq.analytics.metrics.DoubleMetric import DoubleMetric
from sparkdq.exceptions.AnalysisExceptions import AnalysisException
from sparkdq.structures.Entity import Entity


FREQUENCY_COL = "frequency_{}"
MI_COL = "mi_{}_{}"


class MutualInformation(FrequencyBasedAnalyzer):
    
    def __init__(self, column1, column2):
        super(MutualInformation, self).__init__(MutualInformation.__name__, Entity.Multicolumn,
                                                sorted([column1, column2]))

    def __eq__(self, other):
        return self.instance == other.instance

    def __hash__(self):
        return hash((MutualInformation.__name__, ",".join(self.instance)))

    def compute_metric_from_state(self, state):
        cls = DoubleMetric
        if state is None:
            return metric_from_empty(self, cls)
        joint_stats = state.frequencies
        total = state.num_rows
        col1 = self.instance[0]
        col2 = self.instance[1]
        freq_col1 = FREQUENCY_COL.format(col1)
        freq_col2 = FREQUENCY_COL.format(col2)
        count_col = COUNT_COL
        mi_col = MI_COL.format(col1, col2)

        marginal_stats1 = joint_stats\
            .select(col1, count_col)\
            .groupBy(col1)\
            .agg(_sum(count_col).alias(freq_col1))
        marginal_stats2 = joint_stats\
            .select(col2, count_col)\
            .groupBy(col2)\
            .agg(_sum(count_col).alias(freq_col2))

        mi_udf = udf(lambda px, py, pxy: (pxy / total) * math.log((pxy / total) / ((px / total) * (py / total))))
        value = joint_stats\
            .join(marginal_stats1, on=col1)\
            .join(marginal_stats2, on=col2)\
            .withColumn(mi_col, mi_udf(col(freq_col1), col(freq_col2), col(count_col)))\
            .agg(_sum(mi_col))\
            .head()[0]

        if value is None:
            return metric_from_empty(self, cls)
        return metric_from_value(self, cls, value)

    def to_failure_metric(self, exception):
        cls = DoubleMetric
        return metric_from_failure(self, cls, AnalysisException.wrap_if_necessary(exception))

    def to_json(self):
        return {
            "AnalyzerName": MutualInformation.__name__,
            "Columns": self.instance
        }

    @staticmethod
    def from_json(d):
        return MutualInformation(d["Columns"][0], d["Columns"][1])
