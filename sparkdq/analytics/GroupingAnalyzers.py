from abc import abstractmethod

from pyspark.sql.functions import col, count, expr, lit

from sparkdq.analytics.Analyzer import GroupingAnalyzer
from sparkdq.analytics.CommonFunctions import COUNT_COL, metric_from_value, metric_from_empty, if_no_nulls_in
from sparkdq.analytics.metrics.DoubleMetric import DoubleMetric
from sparkdq.analytics.states.FrequenciesAndRows import FrequenciesAndRows


class FrequencyBasedAnalyzer(GroupingAnalyzer):
    """
    Base class for all analytics that operate the frequencies of groups in the data
    """
    def additional_preconditions(self):
        return []

    def compute_metric_from_state(self, state):
        pass

    def compute_state_from_data(self, data, num_rows=None):
        return FrequencyBasedAnalyzer.compute_frequencies(data, self.instance, num_rows)

    def to_json(self):
        pass

    @staticmethod
    def from_json(d):
        pass

    @staticmethod
    def compute_frequencies(data, grouping_columns, num_rows=None):
        """
        Compute frequencies of values of grouping columns
        :param data: source data of DataFrame
        :param grouping_columns: grouping columns
        :param num_rows: number of rows
        :return:
        """
        if num_rows is None:
            num_rows = data.count()
        no_grouping_column_is_null = expr(str(True))
        for column in grouping_columns:
            no_grouping_column_is_null = no_grouping_column_is_null & col(column).isNotNull()
        projection_columns = grouping_columns + [COUNT_COL]
        frequencies = data.select(grouping_columns)\
            .where(no_grouping_column_is_null)\
            .groupby(grouping_columns)\
            .agg(count(lit(1)).alias(COUNT_COL))\
            .select(projection_columns)
        return FrequenciesAndRows(frequencies, num_rows)


class ScanShareableFrequencyBasedAnalyzer(FrequencyBasedAnalyzer):
    """
    Base class for all analytics that compute a (shareable) aggregation over the grouped data
    """
    def compute_metric_from_state(self, state):
        cls = DoubleMetric
        if state is None:
            return metric_from_empty(self, cls)
        aggregations = self.aggregation_functions(state.num_rows)
        result = state.frequencies.agg(*aggregations).collect()[0]
        return self.compute_metric_from_aggregation_result(result, 0)

    def compute_metric_from_aggregation_result(self, result, offset=0):
        cls = DoubleMetric
        if if_no_nulls_in(result, offset):
            return metric_from_value(self, cls, result[offset])
        return metric_from_empty(self, cls)

    @abstractmethod
    def aggregation_functions(self, num_rows):
        pass
