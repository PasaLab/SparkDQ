from pyspark.sql.types import *

from sparkdq.analytics.analyzers.ApproxCountDistinct import ApproxCountDistinct
from sparkdq.analytics.analyzers.ApproxQuantile import ApproxQuantile
from sparkdq.analytics.analyzers.Completeness import Completeness
from sparkdq.analytics.analyzers.DataType import DataType as DataTypeAnalyzer, DataTypeHistogram
from sparkdq.analytics.analyzers.Histogram import Histogram
from sparkdq.analytics.analyzers.Maximum import Maximum
from sparkdq.analytics.analyzers.Mean import Mean
from sparkdq.analytics.analyzers.Minimum import Minimum
from sparkdq.analytics.analyzers.Size import Size
from sparkdq.analytics.analyzers.Sum import Sum
from sparkdq.analytics.analyzers.StandardDeviation import StandardDeviation
from sparkdq.analytics.runners.AnalysisRunner import AnalysisRunner
from sparkdq.exceptions.AnalysisExceptions import AnalysisException
from sparkdq.profiling.Profiles import *
from sparkdq.profiling.Statistics import *
from sparkdq.structures.DataTypeInstances import DataTypeInstances
from sparkdq.structures.Distribution import Distribution
from sparkdq.structures.DistributionValue import DistributionValue


DEFAULT_CARDINALITY_THRESHOLD = 120


class Profiler:

    @staticmethod
    def profile(data, specified_columns=None, low_cardinality_threshold=DEFAULT_CARDINALITY_THRESHOLD):
        """
        Data profiling
        :param data:                        DaraFrame
        :param specified_columns:           specified columns for profile, if None means all columns
        :param low_cardinality_threshold:   distinctness threshold to calculate histogram
        :return:
        """
        if specified_columns is None:
            columns = data.schema.names
        else:
            for col in specified_columns:
                assert col in data.schema.names, "Unable to find the specified column {}".format(col)
            # filter target columns
            columns = list(filter(lambda c: c in specified_columns, data.schema.names))
            assert len(columns) >= 1, "At least contain one valid column"

        # first pass: generic profiling
        analyzers_for_generic_stats, known_types = Profiler.get_analyzers_for_generic_stats_and_known_types(data.schema,
                                                                                                            columns)
        generic_stats_result = AnalysisRunner\
            .on_data(data)\
            .add_analyzers(analyzers_for_generic_stats)\
            .add_analyzer(Size())\
            .run()
        generic_statistics = Profiler.extract_generic_statistics(generic_stats_result, known_types)

        # second pass: numeric profiling
        data = Profiler.cast_numeric_columns(data, columns, generic_statistics)
        analyzers_for_numeric_stats = Profiler.get_analyzers_for_numeric_stats(columns, generic_statistics)
        numeric_stats_result = AnalysisRunner\
            .on_data(data)\
            .add_analyzers(analyzers_for_numeric_stats)\
            .run()
        numeric_statistics = Profiler.extract_numeric_statistics(numeric_stats_result)

        # third pass: categorical profiling
        columns_for_histograms = Profiler.get_columns_for_histograms(generic_statistics, low_cardinality_threshold)
        histograms = Profiler.get_histograms(data, columns_for_histograms, generic_statistics.num_records)
        categorical_statistics = CategoricalStatistics(histograms)
        return Profiler.create_profiles(columns, generic_statistics, numeric_statistics, categorical_statistics)

    @staticmethod
    def get_analyzers_for_generic_stats_and_known_types(schema, columns):
        analyzers = []
        known_types = dict()
        for c in columns:
            data_type = schema[c].dataType
            if isinstance(data_type, StringType):
                analyzers.extend([Completeness(c), ApproxCountDistinct(c), DataTypeAnalyzer(c)])
            else:
                analyzers.extend([Completeness(c), ApproxCountDistinct(c)])
                if isinstance(data_type, ShortType) or isinstance(data_type, IntegerType) or \
                        isinstance(data_type, LongType):
                    known_types[c] = DataTypeInstances.Integral
                elif isinstance(data_type, DecimalType) or isinstance(data_type, FloatType) or \
                        isinstance(data_type, DoubleType):
                    known_types[c] = DataTypeInstances.Fractional
                elif isinstance(data_type, BooleanType):
                    known_types[c] = DataTypeInstances.Boolean
                elif isinstance(data_type, TimestampType):
                    known_types[c] = DataTypeInstances.String
                else:
                    known_types[c] = DataTypeInstances.Unknown
        return analyzers, known_types

    @staticmethod
    def cast_numeric_columns(data, columns, generic_statistics):
        for c in columns:
            col_type = generic_statistics.get_column_type(c)
            if col_type == DataTypeInstances.Integral:
                data = data.withColumn(c, data[c].cast(LongType()))
            elif col_type == DataTypeInstances.Fractional:
                data = data.withColumn(c, data[c].cast(DoubleType()))
        return data

    @staticmethod
    def get_analyzers_for_numeric_stats(columns, generic_statistics):
        analyzers = []
        for c in columns:
            col_type = generic_statistics.get_column_type(c)
            if col_type in [DataTypeInstances.Integral, DataTypeInstances.Fractional]:
                analyzers.extend([Minimum(c), Maximum(c), Mean(c), StandardDeviation(c), Sum(c),
                                  ApproxQuantile(c, list(map(lambda x: x / 100, range(1, 101))))])
        return analyzers

    @staticmethod
    def get_columns_for_histograms(generic_statistics, low_cardinality_threshold):
        target_columns = []
        valid_data_types = [DataTypeInstances.Integral, DataTypeInstances.Fractional, DataTypeInstances.Boolean,
                            DataTypeInstances.String]
        for k, v in generic_statistics.approx_count_distincts.items():
            if (generic_statistics.get_column_type(k) in valid_data_types) and (v <= low_cardinality_threshold):
                target_columns.append(k)
        return target_columns

    @staticmethod
    def extract_generic_statistics(result, known_types):
        num_records = 0
        completenesses = {}
        approx_count_distincts = {}
        inferred_types_and_counts = {}

        for analyzer, metric in result.metric_map.items():
            if isinstance(analyzer, Size):
                num_records = Profiler.filter_exception(metric.value)
            elif isinstance(analyzer, Completeness):
                column = analyzer.instance[0]
                completenesses[column] = Profiler.filter_exception(metric.value)
            elif isinstance(analyzer, DataTypeAnalyzer):
                histogram_distribution = Profiler.filter_exception(metric.value)
                if histogram_distribution is not None:
                    inferred_type = DataTypeHistogram.infer_type(histogram_distribution)
                    # 所有类型的统计
                    counts = dict(map(lambda kv: (kv[0], kv[1].absolute), histogram_distribution.values.items()))
                    inferred_types_and_counts[analyzer.instance] = (inferred_type, counts)
            elif isinstance(analyzer, ApproxCountDistinct):
                approx_count_distincts[analyzer.instance] = Profiler.filter_exception(metric.value)
        return GenericStatistics(num_records, completenesses, approx_count_distincts, inferred_types_and_counts,
                                 known_types)

    @staticmethod
    def extract_numeric_statistics(result):
        minimums = {}
        maximums = {}
        means = {}
        stddevs = {}
        sums = {}
        approx_quantiles = {}

        for analyzer, metric in result.metric_map.items():
            if isinstance(analyzer, Minimum):
                minimums[analyzer.instance] = Profiler.filter_exception(metric.value)
            elif isinstance(analyzer, Maximum):
                maximums[analyzer.instance] = Profiler.filter_exception(metric.value)
            elif isinstance(analyzer, Mean):
                means[analyzer.instance] = Profiler.filter_exception(metric.value)
            elif isinstance(analyzer, StandardDeviation):
                stddevs[analyzer.instance] = Profiler.filter_exception(metric.value)
            elif isinstance(analyzer, Sum):
                sums[analyzer.instance] = Profiler.filter_exception(metric.value)
            elif isinstance(analyzer, ApproxQuantile):
                if Profiler.filter_exception(metric.value) is None:
                    approx_quantiles[analyzer.instance] = None
                else:
                    approx_quantiles[analyzer.instance] = sorted(metric.value.values())
        return NumericStatistics(minimums, maximums, means, stddevs, sums, approx_quantiles)

    @staticmethod
    def get_histograms(data, columns, num_records):
        if len(columns) == 0:
            return {}
        names_to_index = dict(zip(data.schema.names, range(0, len(data.schema.names))))

        def _count(row):
            return list(map(
                lambda c: ((c, str(row[names_to_index[c]]) if row[names_to_index[c]] else Histogram.NULL_VALUE), 1),
                columns
            ))
        counts = data.rdd.flatMap(_count).countByKey()
        counts_per_column = {}
        for k, v in counts.items():
            col = k[0]
            val = k[1]
            if col in counts_per_column:
                counts_per_column[col][val] = v
            else:
                counts_per_column[col] = {val: v}
        histograms = {}
        for k, v in counts_per_column.items():
            values = {}
            for x, y in v.items():
                values[x] = DistributionValue(y, y / num_records)
            histograms[k] = Distribution(values, len(v))
        return histograms

    @staticmethod
    def create_profiles(columns, generic_statistics, numeric_statistics, categorical_statistics):
        profiles = {}
        for col in columns:
            completeness = generic_statistics.completenesses[col]
            approx_count_distinct = generic_statistics.approx_count_distincts[col]
            data_type = generic_statistics.get_column_type(col)
            is_inferred_type = col in generic_statistics.inferred_types_and_counts
            histogram = categorical_statistics.histograms[col] if col in categorical_statistics.histograms.keys() \
                else None
            if col in generic_statistics.inferred_types_and_counts:
                type_counts = generic_statistics.inferred_types_and_counts[col][1]
            else:
                type_counts = {}

            if data_type in [DataTypeInstances.Integral, DataTypeInstances.Fractional]:
                profiles[col] = NumericProfile(col, completeness, approx_count_distinct, data_type, is_inferred_type,
                                               type_counts, histogram, numeric_statistics.minimums[col],
                                               numeric_statistics.maximums[col], numeric_statistics.sums[col],
                                               numeric_statistics.means[col], numeric_statistics.stddevs[col],
                                               numeric_statistics.approx_quantiles[col])
            else:
                profiles[col] = StandardProfile(col, completeness, approx_count_distinct, data_type, is_inferred_type,
                                                type_counts, histogram)
        return Profiles(profiles, generic_statistics.num_records)

    @staticmethod
    def filter_exception(value):
        """
        Filter exceptions when profiling
        :param value:   metric of Analyzer or exception
        :return:
        """
        if isinstance(value, AnalysisException):
            return None
        return value
