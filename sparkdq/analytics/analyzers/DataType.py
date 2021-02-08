from sparkdq.analytics.Analyzer import ScanShareableAnalyzer
from sparkdq.analytics.catalyst.DQFunctions import stateful_data_type
from sparkdq.analytics.CommonFunctions import if_no_nulls_in, metric_from_failure, metric_from_value, metric_from_empty
from sparkdq.analytics.metrics.HistogramMetric import HistogramMetric
from sparkdq.analytics.states.DataTypeHistogram import DataTypeHistogram
from sparkdq.exceptions.AnalysisExceptions import AnalysisException
from sparkdq.structures.Entity import Entity


class DataType(ScanShareableAnalyzer):

    def __init__(self, column, where=None):
        super(DataType, self).__init__(DataType.__name__, Entity.Column, column)
        self.where = where

    def __eq__(self, other):
        return (self.instance == other.instance) and (self.where == other.where)

    def __hash__(self):
        return hash((DataType.__name__, self.instance, self.where))

    def additional_preconditions(self):
        return []

    def aggregation_functions(self):
        return [stateful_data_type(self.instance)]

    def fetch_state_from_aggregation_result(self, result, offset):
        if if_no_nulls_in(result, offset):
            return DataTypeHistogram.from_bytes(result[offset])
        return None

    def compute_metric_from_state(self, state):
        cls = HistogramMetric
        if state is None:
            return metric_from_empty(self, cls)
        return metric_from_value(self, cls, DataTypeHistogram.to_distribution(state))

    def to_failure_metric(self, exception):
        cls = HistogramMetric
        return metric_from_failure(self, cls, AnalysisException.wrap_if_necessary(exception))

    def to_json(self):
        return {
            "AnalyzerName": DataType.__name__,
            "Column": self.instance,
            "Where": self.where
        }

    @staticmethod
    def from_json(d):
        return DataType(d["Column"], d["Where"])
