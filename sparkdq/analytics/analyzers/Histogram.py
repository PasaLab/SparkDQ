from pyspark.sql.functions import col, desc

from sparkdq.analytics.Analyzer import ComplexAnalyzer
from sparkdq.analytics.CommonFunctions import metric_from_failure, metric_from_value, metric_from_empty
from sparkdq.analytics.metrics.HistogramMetric import HistogramMetric
from sparkdq.analytics.states.FrequenciesAndRows import FrequenciesAndRows
from sparkdq.exceptions.AnalysisExceptions import AnalyzerSerializationException, IllegalAnalyzerParameterException, \
    AnalysisException
from sparkdq.structures.Entity import Entity
from sparkdq.structures.Distribution import Distribution
from sparkdq.structures.DistributionValue import DistributionValue


class Histogram(ComplexAnalyzer):
    MAX_ALLOWED_DETAIL_BINS = 1000
    NULL_VALUE = "NullValue"

    def __init__(self, column, binning_udf, max_bins):
        super(Histogram, self).__init__(Histogram.__name__, Entity.Column, column)
        self.binning_udf = binning_udf
        self.max_bins = max_bins

    def __eq__(self, other):
        return (self.instance == other.instance) and \
               (self.binning_udf == other.binning_udf) and \
               (self.max_bins == other.max_bins)

    def __hash__(self):
        return hash((Histogram.__name__, self.instance, self.binning_udf, self.max_bins))

    def additional_preconditions(self):
        return [Histogram.param_check(self.max_bins, self.binning_udf)]

    def to_failure_metric(self, exception):
        cls = HistogramMetric
        return metric_from_failure(self, cls, AnalysisException.wrap_if_necessary(exception))

    def compute_state_from_data(self, data, num_rows=None, state_provider=None):
        if num_rows is None:
            num_rows = data.count()
        if self.binning_udf is None:
            frequencies = data
        else:
            frequencies = data.withColumn(self.instance, self.binning_udf(self.instance))
        frequencies = frequencies\
            .select(col(self.instance).cast("string"))\
            .fillna(Histogram.NULL_VALUE)\
            .groupby(self.instance)\
            .count()
        return FrequenciesAndRows(frequencies, num_rows)

    def compute_metric_from_state(self, state):
        cls = HistogramMetric
        if state is None:
            return metric_from_empty(self, cls)
        top_rows = state.frequencies.sort(desc("count")).head(self.max_bins)
        num_bins = state.frequencies.count()
        histogram_details = {}
        for row in top_rows:
            ratio = row[1] / state.num_rows
            histogram_details[row[0]] = DistributionValue(row[1], ratio)
        return metric_from_value(self, cls, Distribution(histogram_details, num_bins))

    @staticmethod
    def param_check(max_bins, binning_udf):
        def _param_check(_):
            if max_bins > Histogram.MAX_ALLOWED_DETAIL_BINS:
                raise IllegalAnalyzerParameterException("Set too many count bins as {}, the maximum set is {}!".format(
                    max_bins, Histogram.MAX_ALLOWED_DETAIL_BINS))
            if (binning_udf is not None) and (not callable(binning_udf)):
                raise IllegalAnalyzerParameterException("Binning udf should be None or udf function, "
                                                        "but {} was found!".format(str(binning_udf)))
        return _param_check

    def to_json(self):
        if self.binning_udf is None:
            return {
                "AnalyzerName": Histogram.__name__,
                "Column": self.instance,
                "MaxBins": self.max_bins
            }
        else:
            raise AnalyzerSerializationException("Unable to serialize Histogram with binningUdf!")

    @staticmethod
    def from_json(d):
        return Histogram(d["Column"], None, d["MaxBins"])
