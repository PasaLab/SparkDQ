from sparkdq.models.CommonUtils import DEFAULT_INDEX_COL
from sparkdq.repairs.CommonUtils import KeepType
from sparkdq.repairs.transformers.filters.DataTypeFilter import DataTypeFilter, DataTypeDroper
from sparkdq.repairs.transformers.filters.ExpFilter import ExpFilter, ExpDroper
from sparkdq.repairs.transformers.filters.NullFilter import NullFilter
from sparkdq.repairs.transformers.filters.RangeFilter import RangeFilter, RangeDroper
from sparkdq.repairs.transformers.filters.RegexFilter import RegexFilter, RegexDroper
from sparkdq.repairs.transformers.replacers.DataTypeReplacer import DataTypeReplacer
from sparkdq.repairs.transformers.replacers.ExpReplacer import ExpReplacer
from sparkdq.repairs.transformers.replacers.NullFiller import NullFiller
from sparkdq.repairs.transformers.replacers.RangeReplacer import RangeReplacer
from sparkdq.repairs.transformers.replacers.RegexReplacer import RegexReplacer
from sparkdq.repairs.transformers.replacers.RegexSubExtractor import RegexSubExtractor
from sparkdq.repairs.transformers.replacers.RegexSubReplacer import RegexSubReplacer
from sparkdq.repairs.transformers.complex_repairers.BayesianNullFiller import BayesianNullFiller
from sparkdq.repairs.transformers.complex_repairers.DeDuplicator import DeDuplicator
from sparkdq.repairs.transformers.complex_repairers.EntityExtractor import EntityExtractor
from sparkdq.repairs.transformers.complex_repairers.FDRepairer import FDRepairer
from sparkdq.repairs.transformers.complex_repairers.OutlierFilter import OutlierFilter
from sparkdq.repairs.transformers.complex_repairers.OutlierReplacer import OutlierReplacer
from sparkdq.utils.RegularExpressions import *


class Clean:
    """
    One group repair tasks called in RepairSuite.
    """
    def __init__(self, description, transformers=None):
        self.description = description
        self.transformers = list(transformers) if transformers is not None else []

    def _add_transformer(self, transformer):
        self.transformers.append(transformer)
        return self

    """
    handle completeness questions
    """
    def drop_null(self, column):
        transformer = NullFilter(column)
        return self._add_transformer(transformer)

    def fill_null(self, column, way, value=None):
        transformer = NullFiller(column, way, value)
        return self._add_transformer(transformer)

    def impute_null(self, column, dependent_column_or_columns, allowed_values=[], index_col=DEFAULT_INDEX_COL):
        transformer = BayesianNullFiller(column, dependent_column_or_columns, allowed_values, index_col)
        return self._add_transformer(transformer)

    """
    handle duplication questions
    """
    def drop_duplicates(self, column_or_columns, keep=KeepType.DEFAULT.value, order_by=DEFAULT_INDEX_COL):
        transformer = DeDuplicator(column_or_columns, keep, order_by)
        return self._add_transformer(transformer)

    def extract_entities(self, column_or_columns, index_col=DEFAULT_INDEX_COL):
        transformer = EntityExtractor(column_or_columns, index_col)
        return self._add_transformer(transformer)

    """
    handle consistency questions
    """
    def filter_by_exp(self, exp_str):
        transformer = ExpFilter(exp_str)
        return self._add_transformer(transformer)

    def drop_by_exp(self, exp_str):
        transformer = ExpDroper(exp_str)
        return self._add_transformer(transformer)

    def filter_positive(self, column):
        transformer = RangeFilter(column, 0, False, None, False)
        return self._add_transformer(transformer)

    def filter_non_negative(self, column):
        transformer = RangeFilter(column, 0, True, None, False)
        return self._add_transformer(transformer)

    def filter_by_lower_bound(self, column, lower_bound, lower_closed):
        transformer = RangeFilter(column, lower_bound, lower_closed, None, False)
        return self._add_transformer(transformer)

    def filter_by_upper_bound(self, column, upper_bound, upper_closed):
        transformer = RangeFilter(column, None, False, upper_bound, upper_closed)
        return self._add_transformer(transformer)

    def filter_by_range(self, column, lower_bound, lower_closed, upper_bound, upper_closed):
        transformer = RangeFilter(column, lower_bound, lower_closed, upper_bound, upper_closed)
        return self._add_transformer(transformer)

    def drop_by_range(self, column, lower_bound, lower_closed, upper_bound, upper_closed):
        transformer = RangeDroper(column, lower_bound, lower_closed, upper_bound, upper_closed)
        return self._add_transformer(transformer)

    def filter_by_values(self, column, values):
        exp = column + " in ('" + "','".join(values) + "')"
        transformer = ExpFilter(exp)
        return self._add_transformer(transformer)

    def drop_by_values(self, column, values):
        exp = column + " not in ('" + "','".join(values) + "')"
        transformer = ExpFilter(exp)
        return self._add_transformer(transformer)

    def filter_by_data_type(self, column, data_type):
        transformer = DataTypeFilter(column, data_type)
        return self._add_transformer(transformer)

    def drop_by_data_type(self, column, data_type):
        transformer = DataTypeDroper(column, data_type)
        return self._add_transformer(transformer)

    def filter_by_pattern(self, column, pattern):
        transformer = RegexFilter(column, pattern)
        return self._add_transformer(transformer)

    def drop_by_pattern(self, column, pattern):
        transformer = RegexDroper(column, pattern)
        return self._add_transformer(transformer)

    def filter_by_begin(self, column, begin_str):
        pattern = "^" + begin_str
        transformer = RegexFilter(column, pattern)
        return self._add_transformer(transformer)

    def drop_by_begin(self, column, begin_str):
        pattern = "^" + begin_str
        transformer = RegexDroper(column, pattern)
        return self._add_transformer(transformer)

    def filter_by_end(self, column, end_str):
        pattern = end_str + "$"
        transformer = RegexFilter(column, pattern)
        return self._add_transformer(transformer)

    def drop_by_end(self, column, end_str):
        pattern = end_str + "$"
        transformer = RegexDroper(column, pattern)
        return self._add_transformer(transformer)

    def filter_by_sub_str(self, column, sub_str):
        pattern = "({})".format(sub_str)
        transformer = RegexFilter(column, pattern)
        return self._add_transformer(transformer)

    def drop_by_sub_str(self, column, sub_str):
        pattern = "({})".format(sub_str)
        transformer = RegexDroper(column, pattern)
        return self._add_transformer(transformer)

    def filter_url(self, column):
        transformer = RegexFilter(column, URL)
        return self._add_transformer(transformer)

    def filter_email(self, column):
        transformer = RegexFilter(column, EMAIL)
        return self._add_transformer(transformer)

    def replace_by_exp(self, column, exp, way, value=None):
        transformer = ExpReplacer(column, exp, way, value)
        return self._add_transformer(transformer)

    def replace_non_positive(self, column, way, value=None):
        transformer = RangeReplacer(column, None, False, 0, True, way, value)
        return self._add_transformer(transformer)

    def replace_negative(self, column, way, value=None):
        transformer = RangeReplacer(column, None, False, 0, False, way, value)
        return self._add_transformer(transformer)

    def replace_by_range(self, column, lower_bound, lower_closed, upper_bound, upper_closed, way, value=None):
        transformer = RangeReplacer(column, lower_bound, lower_closed, upper_bound, upper_closed, way, value)
        return self._add_transformer(transformer)

    def replace_by_values(self, column, values, way, value=None):
        exp = column + " in ('" + "','".join(values) + "')"
        transformer = ExpReplacer(column, exp, way, value)
        return self._add_transformer(transformer)

    def replace_by_excluded_values(self, column, excluded_values, way, value=None):
        exp = column + " not in ('" + "','".join(excluded_values) + "')"
        transformer = ExpReplacer(column, exp, way, value)
        return self._add_transformer(transformer)

    def replace_by_data_type(self, column, data_type, way, value=None):
        transformer = DataTypeReplacer(column, data_type, way, value, False)
        return self._add_transformer(transformer)

    def replace_by_excluded_data_type(self, column, data_type, way, value=None):
        transformer = DataTypeReplacer(column, data_type, way, value, True)
        return self._add_transformer(transformer)

    def replace_by_pattern(self, column, pattern, way, value=None):
        transformer = RegexReplacer(column, pattern, way, value, False)
        return self._add_transformer(transformer)

    def replace_by_excluded_pattern(self, column, pattern, way, value=None):
        transformer = RegexReplacer(column, pattern, way, value, True)
        return self._add_transformer(transformer)

    def replace_by_sub_pattern(self, column, sub_pattern, value):
        transformer = RegexSubReplacer(column, sub_pattern, value)
        return self._add_transformer(transformer)

    def extract_by_sub_pattern(self, column, sub_pattern):
        transformer = RegexSubExtractor(column, sub_pattern)
        return self._add_transformer(transformer)

    def replace_non_url(self, column, way, value=None):
        transformer = RegexReplacer(column, URL, way, value, True)
        return self._add_transformer(transformer)

    def replace_non_email(self, column, way, value=None):
        transformer = RegexReplacer(column, EMAIL, way, value, True)
        return self._add_transformer(transformer)

    def repair_by_fd(self, cfd_or_cfds, index_col=DEFAULT_INDEX_COL, priorities=[]):
        transformer = FDRepairer(cfd_or_cfds, index_col, priorities)
        return self._add_transformer(transformer)

    """
    handle validity questions, filter and replace interface can also handle validity questions
    """
    def drop_outliers(self, column_or_columns, model, model_params, index_col=DEFAULT_INDEX_COL):
        transformer = OutlierFilter(column_or_columns, model, model_params, index_col=index_col)
        return self._add_transformer(transformer)

    def replace_outliers(self, column_or_columns, model, model_params, value_or_values, index_col=DEFAULT_INDEX_COL):
        transformer = OutlierReplacer(column_or_columns, model, model_params, value_or_values, index_col=index_col)
        return self._add_transformer(transformer)
