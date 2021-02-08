from typing import Callable

from sparkdq.analytics.analyzers.Histogram import Histogram
from sparkdq.checks.CheckResult import CheckResult
from sparkdq.checks.CheckStatus import CheckStatus
from sparkdq.constraints.Constraints import Constraints
from sparkdq.constraints.ConstraintStatus import ConstraintStatus
from sparkdq.models.CommonUtils import DEFAULT_INDEX_COL
from sparkdq.outliers.OutlierSolver import OutlierSolver
from sparkdq.structures.Distribution import Distribution
from sparkdq.utils.Assertions import is_one, is_zero, has_no_violations
from sparkdq.utils.RegularExpressions import URL, EMAIL


class Check:
    """
    Declarative APIs for data quality detection.
    Once check for dirty data with check level, description and constraints, also with private check result and status.
    One check contains several tasks and one detection contains several checks.
    """
    def __init__(self, description, constraints=list()):
        self.description = description
        self.constraints = constraints

    def __str__(self):
        return "Description: {}, Constraints: {}".format(self.description, self.constraints)

    def _add_constraint(self, constraint):
        self.constraints.append(constraint)
        return self

    """
    Completeness operators
    """
    def is_complete(self, column, where=None):
        """
        Creates a constraint that asserts on the column's completeness
        :param column:  Column to run the assertion on
        :param where
        :return
        """
        complete_constraint = Constraints.complete_constraint(column, is_one(), where)
        return self._add_constraint(complete_constraint)

    def has_completeness(self, column, assertion=is_one(), where=None):
        """
        Creates a customized constraint that asserts on the column's completeness
        :param column: Column to run the assertion on
        :param assertion: Customized assertion method
        :param where
        :return:
        """
        complete_constraint = Constraints.complete_constraint(column, assertion, where)
        return self._add_constraint(complete_constraint)

    def are_complete(self, columns, where=None):
        complete_constraint = Constraints.complete_constraint(columns, is_one(), where)
        return self._add_constraint(complete_constraint)

    def have_completeness(self, columns, assertion=is_one(), where=None):
        complete_constraint = Constraints.complete_constraint(columns, assertion, where)
        return self._add_constraint(complete_constraint)

    """
    Uniqueness operators
    """
    def is_unique(self, column):
        """
        Create a constraint that asserts on columns uniqueness
        :param column: str
        :return
        """
        unique_constraint = Constraints.uniqueness_constraint(column, is_one())
        return self._add_constraint(unique_constraint)

    def is_primary_key(self, columns):
        unique_constraint = Constraints.uniqueness_constraint(columns, is_one())
        return self._add_constraint(unique_constraint)

    def has_uniqueness(self, column, assertion=is_one()):
        """
        Create a customized constraint that asserts on some columns uniqueness
        :param column: columns to run the assertion on
        :param assertion: Customized assertion method
        :return:
        """
        unique_constraint = Constraints.uniqueness_constraint(column, assertion)
        return self._add_constraint(unique_constraint)

    def have_uniqueness(self, columns, assertion=is_one()):
        unique_constraint = Constraints.uniqueness_constraint(columns, assertion)
        return self._add_constraint(unique_constraint)

    def has_distinctness(self, column, assertion=is_one()):
        """
        Create a constraint that asserts on some columns distinctness
        :param column: Column to run the assertion on
        :param assertion: Customized assertion method
        :return
        """
        distinct_constraint = Constraints.distinctness_constraint(column, assertion)
        return self._add_constraint(distinct_constraint)

    def have_distinctness(self, columns, assertion=is_one()):
        distinct_constraint = Constraints.distinctness_constraint(columns, assertion)
        return self._add_constraint(distinct_constraint)

    def has_unique_ratio(self, column, assertion):
        unique_ratio_constraint = Constraints.unique_ratio_constraint(column, assertion)
        return self._add_constraint(unique_ratio_constraint)

    def have_unique_ratio(self, columns, assertion):
        unique_ratio_constraint = Constraints.unique_ratio_constraint(columns, assertion)
        return self._add_constraint(unique_ratio_constraint)

    def has_count_distinct(self, column, assertion, binning_udf=None, max_bins=Histogram.MAX_ALLOWED_DETAIL_BINS):
        """
        Create a constraint that asserts on some column's distinctness
        :param column: Column to run the assertion on
        :param binning_udf: udf binning functions
        :param max_bins:
        :param assertion: Customized assertion method
        :return:
        """
        count_distinct_constraint = Constraints.histogram_bin_constraint(column, assertion, binning_udf=binning_udf,
                                                                         max_bins=max_bins)
        return self._add_constraint(count_distinct_constraint)

    def has_histogram_values(self, column, assertion: Callable[[Distribution], bool], binning_udf=None,
                             max_bins=Histogram.MAX_ALLOWED_DETAIL_BINS):
        histogram_constraint = Constraints.histogram_constraint(column, assertion, binning_udf, max_bins)
        return self._add_constraint(histogram_constraint)

    def has_approx_count_distinct(self, column, assertion, where=None):
        """
        Create a constraint that asserts on the column's approximate distinctness
        :param column: Column to run the assertion on
        :param assertion: Customized assertion method
        :param where
        :return:
        """
        approx_distinct_constraint = Constraints.approx_count_distinct_constraint(column, assertion, where)
        return self._add_constraint(approx_distinct_constraint)

    def has_entities(self, column_or_columns, assertion, index_col=DEFAULT_INDEX_COL):
        """
        Create a constraint that asserts on some column's duplication based-on the similarity between records,
         i.e. entity resolution
        :return:
        """
        entities_constraint = Constraints.entities_constraint(column_or_columns, assertion, index_col)
        return self._add_constraint(entities_constraint)

    """
    Consistency operators
    """
    def satisfies_expression(self, expression, assertion=is_one(), where=None):
        """
        Create a constraint that asserts a column satisfies a expression
        :param expression: str, SQL-like expression
        :param assertion: func, customized assertion method
        :param where
        :return
        """
        compliance_constraint = Constraints.compliance_constraint(expression, assertion, where)
        return self._add_constraint(compliance_constraint)

    def is_positive(self, column, assertion=is_one(), where=None):
        """
        Create a constraint that asserts a column contains positive values
        :param column: Column to run the assertion on
        :param assertion: assertion method
        :param where
        :return
        """
        expression = "COALESCE({}, 0.0) > 0".format(column)
        return self.satisfies_expression(expression, assertion, where)

    def is_non_negative(self, column, assertion=is_one(), where=None):
        """
        Create a constraint that asserts a column contains no negative values
        :param column: Column to run the assertion on
        :param assertion: assertion method
        :param where
        :return
        """
        expression = "COALESCE({}, 0.0) >= 0".format(column)
        return self.satisfies_expression(expression, assertion, where)

    def is_greater_than(self, column, value, assertion=is_one(), where=None):
        """
        Create a constraint that asserts all values of column is greater than some value
        :param column: Column to run the assertion on
        :param value: Value to compare with
        :param assertion: assertion method
        :param where
        :return
        """
        expression = "{} > {}".format(column, value)
        return self.satisfies_expression(expression, assertion, where)

    def is_greater_than_or_equal_to(self, column, value, assertion=is_one(), where=None):
        """
        Create a constraint that asserts all values of column is greater than or equal to some value
        :param column: Column to run the assertion on
        :param value: Value to compare with
        :param assertion: assertion method
        :param where
        :return
        """
        expression = "{} >= {}".format(column, value)
        return self.satisfies_expression(expression, assertion, where)

    def is_less_than(self, column, value, assertion=is_one(), where=None):
        """
        Create a constraint that asserts all values of column is less than some value
        :param column: Column to run the assertion on
        :param value: Value to compare with
        :param assertion: assertion method
        :param where
        :return
        """
        expression = "{} < {}".format(column, value)
        return self.satisfies_expression(expression, assertion, where)

    def is_less_than_or_equal_to(self, column, value, assertion=is_one(), where=None):
        """
        Create a constraint that asserts that all values of column is less than or equal to some value
        :param column: Column to run the assertion on
        :param value: Value to compare with
        :param assertion: assertion method
        :param where
        :return
        """
        expression = "{} <= {}".format(column, value)
        return self.satisfies_expression(expression, assertion, where)

    def is_in_range(self, column, lower_bound, upper_bound, include_lower_bound=False, include_upper_bound=False,
                    assertion=is_one(), where=None):
        """
        Create a constraint that asserts that all values of column is in some range
        :param column: Column to run assertion on
        :param lower_bound: lower bound of range
        :param upper_bound: upper bound of range
        :param include_lower_bound: whether include lower bound
        :param include_upper_bound: whether include upper bound
        :param assertion: assertion method
        :param where
        :return
        """
        lower_op_str = ">"
        if include_lower_bound:
            lower_op_str += "="
        upper_op_str = "<"
        if include_upper_bound:
            upper_op_str += "="
        expression = "{} {} {} and {} {} {}".format(column, lower_op_str, lower_bound, column, upper_op_str,
                                                    upper_bound)
        return self.satisfies_expression(expression, assertion, where)

    def is_contained_in(self, column, allowed_values, assertion=is_one(), where=None):
        """
        Create a constraint that asserts that all values of column is in specific values
        :param column: Column to run the assertion on
        :param allowed_values: list of specific values
        :param assertion: assertion method
        :param where
        :return
        """
        allowed_values_str = ",".join(["'{}'".format(value) for value in allowed_values])
        expression = "{} in ({})".format(column, allowed_values_str)
        return self.satisfies_expression(expression, assertion, where)

    def has_data_type(self, column, data_type, assertion=is_one(), where=None):
        """
        Create a constraint that asserts that all values of column conform to the given data type
        :param column: Column to run the assertion on
        :param data_type:
        :param assertion: assertion method
        :param where
        :return
        """
        return self._add_constraint(Constraints.data_type_constraint(column, data_type, assertion, where))

    def matches_pattern(self, column, pattern, assertion=is_one(), where=None):
        """
        Create a constraint that asserts all values of column match the regular expression
        :param column: Column to run the assertion on
        :param pattern: The columns values will be checked for a match against this pattern
        :param assertion: assertion method
        :param where: the limited range for checking
        :return
        """
        pattern_match_constraint = Constraints.pattern_match_constraint(column, pattern, assertion, where=where)
        return self._add_constraint(pattern_match_constraint)

    def begins_with(self, column, begin_str, assertion=is_one(), where=None):
        """
        Create a constraint that asserts value of column starts with some string
        :param column: Column to run the assertion on
        :param begin_str: begin string
        :param assertion: assertion method
        :param where
        :return
        """
        pattern = "^{}".format(begin_str)
        return self.matches_pattern(column, pattern, assertion, where)

    def ends_with(self, column, end_str, assertion=is_one(), where=None):
        """
        Asserts that, in each row, the value of column ends with some string
        :param column: Column to run the assertion on
        :param end_str: end with
        :param assertion: assertion method
        :param where
        :return
        """
        pattern = "{}$".format(end_str)
        return self.matches_pattern(column, pattern, assertion, where)

    def contains_str(self, column, contain_str, assertion=is_one(), where=None):
        """
        Asserts that, in each row, the value of column contains some string
        :param column: Column to run the assertion on
        :param contain_str: contained string
        :param assertion: assertion method
        :param where
        :return
        """
        pattern = "({})".format(contain_str)
        return self.matches_pattern(column, pattern, assertion, where)

    def contains_url(self, column, assertion=is_one(), where=None):
        """
        Check to run against the compliance of a column against a URL pattern
        :param column: Column to run the assertion on
        :param assertion: assertion method
        :param where
        :return
        """
        return self.matches_pattern(column, URL, assertion, where)

    def contains_email(self, column, assertion=is_one(), where=None):
        """
        Check to run against the compliance of a column against a EMAIL pattern
        :param column: Column to run the assertion on
        :param assertion: assertion method
        :param where
        :return
        """
        return self.matches_pattern(column, EMAIL, assertion, where)

    def satisfies_fd(self, fd, assertion=has_no_violations(), index_col=DEFAULT_INDEX_COL):
        """
        Create a constraint that asserts data satisfies functional dependencies
        :param fd: str of cfd
        :param assertion:
        :param index_col:
        :return:
        """
        fd_constraint = Constraints.functional_dependency_constraint(fd, assertion, index_col=index_col)
        return self._add_constraint(fd_constraint)

    """
    Validity operators
    """
    def has_size(self, assertion, where=None):
        size_constraint = Constraints.size_constraint(assertion, where)
        return self._add_constraint(size_constraint)

    def has_max(self, column, assertion, where=None):
        max_constraint = Constraints.max_constraint(column, assertion, where)
        return self._add_constraint(max_constraint)

    def has_min(self, column, assertion, where=None):
        min_constraint = Constraints.min_constraint(column, assertion, where)
        return self._add_constraint(min_constraint)

    def has_mean(self, column, assertion, where=None):
        mean_constraint = Constraints.mean_constraint(column, assertion, where)
        return self._add_constraint(mean_constraint)

    def has_mode(self, column, assertion, where=None):
        mode_constraint = Constraints.mode_constraint(column, assertion, where)
        return self._add_constraint(mode_constraint)

    def has_sum(self, column, assertion, where=None):
        sum_constraint = Constraints.sum_constraint(column, assertion, where)
        return self._add_constraint(sum_constraint)

    def has_standard_deviation(self, column, assertion, where=None):
        stddev_constraint = Constraints.standard_deviation_constraint(column, assertion, where)
        return self._add_constraint(stddev_constraint)

    def has_approx_quantile(self, column, quantile, assertion):
        approx_quantile_constraint = Constraints.approx_quantile_constraint(column, quantile, assertion)
        return self._add_constraint(approx_quantile_constraint)

    def has_entropy(self, column, assertion):
        entropy_constraint = Constraints.entropy_constraint(column, assertion)
        return self._add_constraint(entropy_constraint)

    def has_mutual_information(self, column1, column2, assertion):
        mutual_information_constraint = Constraints.mutual_information_constraint(column1, column2, assertion)
        return self._add_constraint(mutual_information_constraint)

    def has_correlation(self, column1, column2, assertion, where=None):
        correlation_constraint = Constraints.correlation_constraint(column1, column2, assertion, where=where)
        return self._add_constraint(correlation_constraint)

    def has_max_length(self, column, assertion, where=None):
        max_length_constraint = Constraints.max_length_constraint(column, assertion, where)
        return self._add_constraint(max_length_constraint)

    def has_min_length(self, column, assertion, where=None):
        min_length_constraint = Constraints.min_length_constraint(column, assertion, where)
        return self._add_constraint(min_length_constraint)

    def has_outliers(self, column_or_columns, model: OutlierSolver, model_params=None, assertion=is_zero(),
                     index_col=DEFAULT_INDEX_COL):
        if model_params is None:
            model_params = model.default_params()
        outlier_constraint = Constraints.outliers_constraint(column_or_columns, model, model_params, assertion,
                                                             index_col)
        return self._add_constraint(outlier_constraint)

    def evaluate(self, analyzer_context):
        """
        Evaluate this check on computed metrics.
        :param analyzer_context: result of metrics computation
        :return: Check result
        """
        constraint_results = [constraint.evaluate(analyzer_context.metric_map) for constraint in self.constraints]
        any_failure = False
        for result in constraint_results:
            if result.status == ConstraintStatus.Failure:
                any_failure = True
                break
        if any_failure:
            check_status = CheckStatus.Failure
        else:
            check_status = CheckStatus.Success
        return CheckResult(self, check_status, constraint_results)

    def required_analyzers(self):
        """
        Each check contains many constraints, each constraint correspond to one analyzer.
        :return: analyzers of this check
        """
        required_analyzers = []
        for constraint in self.constraints:
            required_analyzers.append(constraint.analyzer)
        return required_analyzers
