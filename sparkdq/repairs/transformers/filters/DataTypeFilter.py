from pyspark.sql.functions import col

from sparkdq.analytics.Preconditions import has_column, is_string
from sparkdq.repairs.transformers.filters.Filter import Filter
from sparkdq.structures.ConstrainableDataTypes import ConstrainableDataTypes
from sparkdq.utils.RegularExpressions import FRACTIONAL, INTEGRAL, BOOLEAN


class DataTypeFilter(Filter):

    def __init__(self, column, data_type):
        self.column = column
        self.data_type = data_type.lower()

    def filter_condition(self):
        if self.data_type == ConstrainableDataTypes.Null.value:
            return col(self.column).isNull()
        elif self.data_type == ConstrainableDataTypes.Fractional.value:
            return col(self.column).rlike(FRACTIONAL)
        elif self.data_type == ConstrainableDataTypes.Integral.value:
            return col(self.column).rlike(INTEGRAL)
        elif self.data_type == ConstrainableDataTypes.Boolean.value:
            return col(self.column).rlike(BOOLEAN)
        elif self.data_type == ConstrainableDataTypes.Numeric.value:
            return col(self.column).rlike(INTEGRAL) | col(self.column).rlike(FRACTIONAL)
        else:
            return None

    def preconditions(self):
        return [has_column(self.column), is_string(self.data_type), DataTypeFilter._param_check(self.data_type)]

    @staticmethod
    def _param_check(data_type):
        def _param_check(_):
            supported_types = [ConstrainableDataTypes.Null.value, ConstrainableDataTypes.Fractional.value,
                               ConstrainableDataTypes.Integral.value, ConstrainableDataTypes.Boolean.value,
                               ConstrainableDataTypes.Numeric.value]
            if data_type not in supported_types:
                raise Exception("Unsupported data type {}, only {} are supported now!"
                                .format(data_type, ", ".join(supported_types)))
        return _param_check


class DataTypeDroper(DataTypeFilter):

    def __init__(self, column, data_type):
        super(DataTypeDroper, self).__init__(column, data_type)

    def filter_condition(self):
        condition = super(DataTypeDroper, self).filter_condition()
        return ~condition if condition else condition
