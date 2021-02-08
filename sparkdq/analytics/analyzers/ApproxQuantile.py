from sparkdq.analytics.Analyzer import ScanShareableAnalyzer
from sparkdq.analytics.catalyst.DQFunctions import stateful_approx_quantile
from sparkdq.analytics.CommonFunctions import metric_from_empty, metric_from_value, metric_from_failure, \
    if_no_nulls_in, conditional_selection
from sparkdq.analytics.metrics.KeyedDoubleMetric import KeyedDoubleMetric
from sparkdq.analytics.Preconditions import is_numeric
from sparkdq.analytics.states.ApproxQuantileState import ApproxQuantileState
from sparkdq.exceptions.AnalysisExceptions import IllegalAnalyzerParameterException, AnalysisException
from sparkdq.structures.Entity import Entity


class ApproxQuantile(ScanShareableAnalyzer):
    """
    Approx quantiles return KeyedDoubleMetric containing <quantile, value> pairs.
    """
    def __init__(self, column, quantile_or_quantiles, relative_error=0.01, where=None):
        """
        :param column: str
        :param quantile_or_quantiles: float or list of float
        :param relative_error:
        :param where
        """
        super(ApproxQuantile, self).__init__(ApproxQuantile.__name__, Entity.Column, column)
        if not isinstance(quantile_or_quantiles, list):
            quantile_or_quantiles = [quantile_or_quantiles]
        self.quantiles = quantile_or_quantiles
        if relative_error > 1:
            relative_error = 1
        self.relative_error = relative_error
        self.where = where

    def __eq__(self, other):
        return (self.instance == other.instance) and \
               (self.quantiles == other.quantiles) and \
               (self.relative_error == other.relative_error) and \
               (self.where == other.where)

    def __hash__(self):
        return hash((ApproxQuantile.__name__, self.instance, self.relative_error, self.where))

    def additional_preconditions(self):
        return [is_numeric(self.instance), ApproxQuantile.param_check(self.quantiles, self.relative_error)]

    def aggregation_functions(self):
        return [stateful_approx_quantile(conditional_selection(self.instance, self.where), self.relative_error)]

    def fetch_state_from_aggregation_result(self, result, offset):
        if if_no_nulls_in(result, offset):
            quantile_summaries = ApproxQuantileState.quantile_summaries_from_bytes(result[offset])
            if quantile_summaries.count == 0:
                return None
            return ApproxQuantileState(quantile_summaries)
        return None

    def compute_metric_from_state(self, state):
        # 统一使用键控Metric，验证时需注意
        cls = KeyedDoubleMetric
        if state is None:
            return metric_from_empty(self, cls)
        value = state.quantile_summaries.query_by_quantiles(self.quantiles)
        return metric_from_value(self, cls, value)

    def to_failure_metric(self, exception):
        cls = KeyedDoubleMetric
        return metric_from_failure(self, cls, AnalysisException.wrap_if_necessary(exception))

    @staticmethod
    def param_check(quantiles, relative_error):
        def _param_check(_):
            if len(quantiles) < 1:
                raise IllegalAnalyzerParameterException("Must have at least 1 quantile.")
            for q in quantiles:
                if (q < 0.0) or (q > 1.0):
                    raise IllegalAnalyzerParameterException("Quantile parameter must be in the closed interval [0, 1], "
                                                            "but {} is given!".format(q))
            if relative_error < 0.0:
                raise IllegalAnalyzerParameterException("Relative error parameter must be no less than 0, "
                                                        "but {} is given!".format(relative_error))
        return _param_check

    def to_json(self):
        return {
            "AnalyzerName": ApproxQuantile.__name__,
            "Column": self.instance,
            "Quantiles": self.quantiles,
            "RelativeError": self.relative_error,
            "Where": self.where
        }

    @staticmethod
    def from_json(d):
        return ApproxQuantile(d["Column"], d["Quantiles"], d["RelativeError"], d["Where"])
