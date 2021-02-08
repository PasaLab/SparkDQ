from sparkdq.analytics.analyzers.ApproxCountDistinct import ApproxCountDistinct
from sparkdq.analytics.analyzers.ApproxQuantile import ApproxQuantile
from sparkdq.analytics.analyzers.Completeness import Completeness
from sparkdq.analytics.analyzers.Compliance import Compliance
from sparkdq.analytics.analyzers.Correlation import Correlation
from sparkdq.analytics.analyzers.DataType import DataType
from sparkdq.analytics.analyzers.Distinctness import Distinctness
from sparkdq.analytics.analyzers.Entities import Entities
from sparkdq.analytics.analyzers.Entropy import Entropy
from sparkdq.analytics.analyzers.FunctionalDependency import FunctionalDependency
from sparkdq.analytics.analyzers.Histogram import Histogram
from sparkdq.analytics.analyzers.Maximum import Maximum
from sparkdq.analytics.analyzers.MaxLength import MaxLength
from sparkdq.analytics.analyzers.Mean import Mean
from sparkdq.analytics.analyzers.Minimum import Minimum
from sparkdq.analytics.analyzers.MinLength import MinLength
from sparkdq.analytics.analyzers.Mode import Mode
from sparkdq.analytics.analyzers.MutualInformation import MutualInformation
from sparkdq.analytics.analyzers.Outliers import Outliers
from sparkdq.analytics.analyzers.PatternMatch import PatternMatch
from sparkdq.analytics.analyzers.Size import Size
from sparkdq.analytics.analyzers.StandardDeviation import StandardDeviation
from sparkdq.analytics.analyzers.Sum import Sum
from sparkdq.analytics.analyzers.Uniqueness import Uniqueness
from sparkdq.analytics.analyzers.UniqueRatio import UniqueRatio
from sparkdq.analytics.CommonFunctions import value_picker
from sparkdq.constraints.AnalysisBasedConstraint import AnalysisBasedConstraint
from sparkdq.constraints.Constraint import Constraint
from sparkdq.models.CommonUtils import DEFAULT_INDEX_COL
from sparkdq.structures.ConstrainableDataTypes import ConstrainableDataTypes
from sparkdq.structures.DataTypeInstances import DataTypeInstances


class Constraints:
    """
    All constraints including four categories:
    1. completeness: whether the data has missing values
    2. uniqueness: whether the data has redundant values
    3. consistency: whether the data meets rules
    4. validity: whether the data is valid
    """

    """
    Completeness constraints
    """
    @staticmethod
    def complete_constraint(column_or_columns, assertion, where=None):
        completeness = Completeness(column_or_columns, where)
        name = "CompletenessConstraint({})".format(completeness)
        constraint = AnalysisBasedConstraint(name, completeness, assertion)
        return constraint

    """
    Uniqueness constraints
    """
    @staticmethod
    def uniqueness_constraint(column_or_columns, assertion):
        uniqueness = Uniqueness(column_or_columns)
        name = "UniquenessConstraint({})".format(uniqueness)
        constraint = AnalysisBasedConstraint(name, uniqueness, assertion)
        return constraint

    @staticmethod
    def distinctness_constraint(column_or_columns, assertion):
        distinctness = Distinctness(column_or_columns)
        name = "DistinctnessConstraint({})".format(distinctness)
        constraint = AnalysisBasedConstraint(name, distinctness, assertion)
        return constraint

    @staticmethod
    def unique_ratio_constraint(column_or_columns, assertion):
        unique_ratio = UniqueRatio(column_or_columns)
        name = "UniqueRatioConstraint({})".format(unique_ratio)
        constraint = AnalysisBasedConstraint(name, unique_ratio, assertion)
        return constraint

    @staticmethod
    def histogram_bin_constraint(column, assertion, binning_udf=None, max_bins=Histogram.MAX_ALLOWED_DETAIL_BINS):
        histogram = Histogram(column, binning_udf, max_bins)
        name = "HistogramBinConstraint({})".format(histogram)
        constraint = AnalysisBasedConstraint(name, histogram, assertion, value_picker("number_of_bins"))
        return constraint

    @staticmethod
    def histogram_constraint(column, assertion, binning_udf=None, max_bins=Histogram.MAX_ALLOWED_DETAIL_BINS):
        histogram = Histogram(column, binning_udf, max_bins)
        name = "HistogramConstraint({})".format(histogram)
        constraint = AnalysisBasedConstraint(name, histogram, assertion)
        return constraint

    @staticmethod
    def approx_count_distinct_constraint(column, assertion, where=None):
        approx_count_distinct = ApproxCountDistinct(column, where)
        name = "ApproxCountDistinctConstraint({})".format(approx_count_distinct)
        constraint = AnalysisBasedConstraint(name, approx_count_distinct, assertion)
        return constraint

    @staticmethod
    def entities_constraint(column_or_columns, assertion, index_col=DEFAULT_INDEX_COL):
        entities = Entities(column_or_columns, index_col)
        name = "EntitiesConstraint({})".format(entities)
        constraint = AnalysisBasedConstraint(name, entities, assertion)
        return constraint

    """
    Consistency constraints
    """
    @staticmethod
    def compliance_constraint(expression, assertion, where=None):
        compliance = Compliance(expression, where)
        name = "ComplianceConstraint({})".format(compliance)
        constraint = AnalysisBasedConstraint(name, compliance, assertion)
        return constraint

    @staticmethod
    def pattern_match_constraint(column, pattern, assertion, where=None):
        pattern_match = PatternMatch(column, pattern, where)
        name = "PatternMatchConstraint({})".format(pattern_match)
        constraint = AnalysisBasedConstraint(name, pattern_match, assertion)
        return constraint

    @staticmethod
    def functional_dependency_constraint(cfd_str, assertion, index_col=DEFAULT_INDEX_COL):
        fd = FunctionalDependency(cfd_str, index_col=index_col)
        name = "FunctionalDependencyConstraint({})".format(fd)
        constraint = AnalysisBasedConstraint(name, fd, assertion)
        return constraint

    @staticmethod
    def data_type_constraint(column, data_type, assertion, where=None):
        data_type = ConstrainableDataTypes(data_type)
        if data_type is ConstrainableDataTypes.Null:
            pick_func = Constraint.ratio_of_types(False, DataTypeInstances.Unknown)
        elif data_type is ConstrainableDataTypes.Fractional:
            pick_func = Constraint.ratio_of_types(True, DataTypeInstances.Fractional)
        elif data_type is ConstrainableDataTypes.Integral:
            pick_func = Constraint.ratio_of_types(True, DataTypeInstances.Integral)
        elif data_type is ConstrainableDataTypes.Boolean:
            pick_func = Constraint.ratio_of_types(True, DataTypeInstances.Boolean)
        elif data_type is ConstrainableDataTypes.String:
            pick_func = Constraint.ratio_of_types(True, DataTypeInstances.String)
        else:
            # Numeric Type
            def _numeric(distribution):
                return Constraint.ratio_of_types(True, DataTypeInstances.Fractional)(distribution) + \
                       Constraint.ratio_of_types(True, DataTypeInstances.Integral)(distribution)
            pick_func = _numeric
        name = "DataTypeConstraint({})".format(data_type)
        constraint = AnalysisBasedConstraint(name, DataType(column, where), assertion, pick_func)
        return constraint

    """
    Validity constraints
    """
    @staticmethod
    def size_constraint(assertion, where=None):
        size = Size(where=where)
        name = "SizeConstraint({})".format(size)
        constraint = AnalysisBasedConstraint(name, size, assertion)
        return constraint

    @staticmethod
    def max_constraint(column, assertion, where=None):
        maximum = Maximum(column, where=where)
        name = "MaximumConstraint({})".format(maximum)
        constraint = AnalysisBasedConstraint(name, maximum, assertion)
        return constraint

    @staticmethod
    def min_constraint(column, assertion, where=None):
        minimum = Minimum(column, where=where)
        name = "MinimumConstraint({}".format(minimum)
        constraint = AnalysisBasedConstraint(name, minimum, assertion)
        return constraint

    @staticmethod
    def mean_constraint(column, assertion, where=None):
        mean = Mean(column, where=where)
        name = "MeanConstraint({})".format(mean)
        constraint = AnalysisBasedConstraint(name, mean, assertion)
        return constraint

    @staticmethod
    def mode_constraint(column, assertion, where=None):
        mode = Mode(column, where=where)
        name = "Mode({})".format(mode)
        constraint = AnalysisBasedConstraint(name, mode, assertion)
        return constraint

    @staticmethod
    def sum_constraint(column, assertion, where=None):
        summation = Sum(column, where=where)
        name = "SumConstraint({})".format(summation)
        constraint = AnalysisBasedConstraint(name, summation, assertion)
        return constraint

    @staticmethod
    def standard_deviation_constraint(column, assertion, where=None):
        stddev = StandardDeviation(column, where=where)
        name = "StandardDeviationConstraint({})".format(stddev)
        constraint = AnalysisBasedConstraint(name, stddev, assertion)
        return constraint

    @staticmethod
    def approx_quantile_constraint(column, quantile, assertion):
        # relative_error shouldn't be exposed to user, if you have to use this parameter,
        # directly build the detection models
        # quantile should be only one, otherwise assertion will be inconvenient to define
        def _get_quantile(quantile_map):
            return quantile_map[str(quantile)]
        pick_func = _get_quantile
        approx_quantile = ApproxQuantile(column, quantile)
        name = "ApproxQuantileConstraint({})".format(approx_quantile)
        constraint = AnalysisBasedConstraint(name, approx_quantile, assertion, value_picker=pick_func)
        return constraint

    @staticmethod
    def entropy_constraint(column, assertion):
        entropy = Entropy(column)
        name = "EntropyConstraint({})".format(entropy)
        constraint = AnalysisBasedConstraint(name, entropy, assertion)
        return constraint

    @staticmethod
    def mutual_information_constraint(column1, column2, assertion):
        mutual_information = MutualInformation(column1, column2)
        name = "MutualInformationConstraint({})".format(mutual_information)
        constraint = AnalysisBasedConstraint(name, mutual_information, assertion)
        return constraint

    @staticmethod
    def correlation_constraint(column1, column2, assertion, where=None):
        correlation = Correlation(column1, column2, where=where)
        name = "CorrelationConstraint({})".format(correlation)
        constraint = AnalysisBasedConstraint(name, correlation, assertion)
        return constraint

    @staticmethod
    def max_length_constraint(column, assertion, where=None):
        max_length = MaxLength(column, where)
        name = "MaxLengthConstraint({})".format(max_length)
        constraint = AnalysisBasedConstraint(name, max_length, assertion)
        return constraint

    @staticmethod
    def min_length_constraint(column, assertion, where=None):
        min_length = MinLength(column, where)
        name = "MinLengthConstraint({})".format(column)
        constraint = AnalysisBasedConstraint(name, min_length, assertion)
        return constraint

    @staticmethod
    def outliers_constraint(columns, model, model_params, assertion, index_col=DEFAULT_INDEX_COL, where=None):
        outliers = Outliers(columns, model, model_params, index_col=index_col, where=where)
        name = "Outliers({})".format(outliers)
        constraint = AnalysisBasedConstraint(name, outliers, assertion)
        return constraint
