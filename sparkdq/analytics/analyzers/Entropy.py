import math

from pyspark.sql.functions import udf, sum as _sum, col

from sparkdq.analytics.GroupingAnalyzers import ScanShareableFrequencyBasedAnalyzer
from sparkdq.structures.Entity import Entity


class Entropy(ScanShareableFrequencyBasedAnalyzer):

    def __init__(self, column):
        super(Entropy, self).__init__(Entropy.__name__, Entity.Multicolumn, column)

    def __eq__(self, other):
        return self.instance == other.instance

    def __hash__(self):
        return hash((Entropy.__name__, self.instance))

    def aggregation_functions(self, num_rows):
        entropy_udf = udf(lambda count: 0.0 if count == 0.0 else -(count / num_rows) * math.log(count / num_rows))
        return [_sum(entropy_udf(col(self.instance)))]

    def to_json(self):
        return {
            "AnalyzerName": Entropy.__name__,
            "Column": self.instance
        }

    @staticmethod
    def from_json(d):
        return Entropy(d["Column"])
