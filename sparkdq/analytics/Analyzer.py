from abc import abstractmethod

from sparkdq.analytics.CommonFunctions import metric_from_failure, metric_from_empty, metric_from_value
from sparkdq.analytics.metrics.DoubleMetric import DoubleMetric
from sparkdq.analytics.Preconditions import has_column, has_columns
from sparkdq.exceptions.AnalysisExceptions import AnalysisException


class Analyzer:

    def __init__(self, name, entity, instance):
        self.name = name
        self.entity = entity
        self.instance = instance

    @abstractmethod
    def preconditions(self):
        pass

    @abstractmethod
    def compute_state_from_data(self, data):
        pass

    @abstractmethod
    def compute_metric_from_state(self, state):
        pass

    @abstractmethod
    def to_failure_metric(self, exception):
        pass

    @abstractmethod
    def to_json(self):
        pass

    @staticmethod
    @abstractmethod
    def from_json(d):
        pass

    def compute_metric_from_data(self, data):
        try:
            for condition in self.preconditions():
                condition(data.schema)
            state = self.compute_state_from_data(data)
            return self.compute_metric_from_state(state)
        except Exception as e:
            return self.to_failure_metric(AnalysisException.wrap_if_necessary(e))


class ScanShareableAnalyzer(Analyzer):
    """
    Scan-shareable analyzer, further classified into StandardScanShareableAnalyzer and others
    """
    @abstractmethod
    def additional_preconditions(self):
        pass

    def preconditions(self):
        common_preconditions = [has_columns(self.instance) if isinstance(self.instance, list)
                                else has_column(self.instance)]
        return common_preconditions + self.additional_preconditions()

    @abstractmethod
    def aggregation_functions(self):
        pass

    @abstractmethod
    def fetch_state_from_aggregation_result(self, result, offset):
        pass

    def compute_state_from_data(self, data):
        aggregations = self.aggregation_functions()
        results = data.agg(*aggregations).collect()[0]
        return self.fetch_state_from_aggregation_result(results, 0)

    def compute_metric_from_aggregation_result(self, result, offset, state_provider=None):
        state = self.fetch_state_from_aggregation_result(result, offset)
        if state_provider is not None:
            state_provider.persist(self, state)
        return self.compute_metric_from_state(state)


class StandardScanShareableAnalyzer(ScanShareableAnalyzer):
    """
    Standard scan-shareable analyzers return DoubleMetric
    """
    @abstractmethod
    def additional_preconditions(self):
        pass

    def compute_metric_from_state(self, state):
        cls = DoubleMetric
        if state is None:
            return metric_from_empty(self, cls)
        return metric_from_value(self, cls,  state.metric_value())

    def to_failure_metric(self, exception):
        cls = DoubleMetric
        return metric_from_failure(self, cls, exception)


class GroupingAnalyzer(Analyzer):
    """
    Grouping analyzers, further classified into FrequencyBasedAnalyzer and others(currently only FrequencyBasedAnalyzer)
    """
    @abstractmethod
    def additional_preconditions(self):
        pass

    def preconditions(self):
        return [has_columns(self.instance)] + self.additional_preconditions()

    def to_failure_metric(self, exception):
        cls = DoubleMetric
        return metric_from_failure(self, cls, exception)

    def grouping_columns(self):
        return self.instance


class ComplexAnalyzer(Analyzer):
    """
    Complex analyzers can't be solved by scanning and grouping, such as Outlier, FD, EntityResolution
    """
    @abstractmethod
    def compute_state_from_data(self, data, num_rows=None, state_provider=None):
        pass

    def preconditions(self):
        common_preconditions = [has_columns(self.instance) if isinstance(self.instance, list)
                                else has_column(self.instance)]
        return common_preconditions + self.additional_preconditions()

    @abstractmethod
    def additional_preconditions(self):
        pass

    def compute_metric_from_data(self, data, num_rows=None, state_provider=None):
        try:
            for condition in self.preconditions():
                condition(data.schema)
            state = self.compute_state_from_data(data, num_rows, state_provider)
            return self.compute_metric_from_state(state)
        except Exception as e:
            return self.to_failure_metric(AnalysisException.wrap_if_necessary(e))
