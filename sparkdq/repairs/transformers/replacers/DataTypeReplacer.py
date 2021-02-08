from pyspark.sql.functions import col

from sparkdq.analytics.Preconditions import is_string
from sparkdq.repairs.transformers.replacers.Replacer import Replacer
from sparkdq.structures.ConstrainableDataTypes import ConstrainableDataTypes
from sparkdq.utils.RegularExpressions import FRACTIONAL, INTEGRAL, BOOLEAN


class DataTypeReplacer(Replacer):

    def __init__(self, column, data_type, way, value=None, negative=False):
        super(DataTypeReplacer, self).__init__(column, way, value)
        self.data_type = data_type
        self.negative = negative

    def select_condition(self):
        if self.data_type == ConstrainableDataTypes.Null.value:
            return col(self.column).isNull() if not self.negative else col(self.column).isNotNull()
        elif self.data_type == ConstrainableDataTypes.Fractional.value:
            return col(self.column).rlike(FRACTIONAL) if not self.negative else ~col(self.column).rlike(FRACTIONAL)
        elif self.data_type == ConstrainableDataTypes.Integral.value:
            return col(self.column).rlike(INTEGRAL) if not self.negative else ~col(self.column).rlike(INTEGRAL)
        elif self.data_type == ConstrainableDataTypes.Numeric.value:
            return (col(self.column).rlike(FRACTIONAL) | col(self.column).rlike(INTEGRAL)) if not self.negative \
                else ~(col(self.column).rlike(INTEGRAL) | col(self.column).rlike(FRACTIONAL))
        else:
            return col(self.column).rlike(BOOLEAN) if not self.negative else ~col(self.column).rlike(BOOLEAN)

    def preconditions(self):
        return [is_string(self.column), DataTypeReplacer._param_check(self.data_type),
                Replacer._str_param_check(self.way, self.value)] + self.common_preconditions()

    @staticmethod
    def _param_check(data_type):
        def _param_check(_):
            supported_types = [ConstrainableDataTypes.Null.value, ConstrainableDataTypes.Fractional.value,
                               ConstrainableDataTypes.Integral.value, ConstrainableDataTypes.Boolean.value]
            if data_type not in supported_types:
                raise Exception("Unsupported data type {}, only {} are supported now!"
                                .format(data_type, ", ".join(supported_types)))
        return _param_check
