from pyspark.sql.functions import rank, col, row_number
from pyspark.sql.window import Window

from sparkdq.analytics.Preconditions import has_columns, has_column
from sparkdq.exceptions.TransformExceptions import IllegalTransformerParameterException
from sparkdq.models.CommonUtils import DEFAULT_INDEX_COL
from sparkdq.repairs.CommonUtils import KeepType
from sparkdq.repairs.transformers.complex_repairers.ComplexRepairer import ComplexRepairer

WINDOW_RANK_COL = "window_rank"


class DeDuplicator(ComplexRepairer):

    def __init__(self, column_or_columns, keep=KeepType.DEFAULT.value, order_by=DEFAULT_INDEX_COL):
        if not isinstance(column_or_columns, list):
            column_or_columns = [column_or_columns]
        self.columns = column_or_columns
        self.keep = keep.lower()
        self.order_by = order_by

    def preconditions(self):
        return [has_columns(self.columns), DeDuplicator._param_check(self.keep), has_column(self.order_by)]

    def transform(self, df, state_provider=None, check=True):
        if check:
            for condition in self.preconditions():
                condition(df.schema)
        if self.keep == KeepType.DEFAULT.value:
            return df.drop_duplicates(self.columns)
        else:
            if self.keep == KeepType.FIRST.value:
                window = Window.partitionBy(self.columns).orderBy(col(self.order_by))
            else:
                window = Window.partitionBy(self.columns).orderBy(col(self.order_by).desc())
            return df.select('*', row_number().over(window).alias(WINDOW_RANK_COL))\
                .where('{} == 1'.format(WINDOW_RANK_COL)).drop(WINDOW_RANK_COL)

    @staticmethod
    def _param_check(keep):
        def _param_check(_):
            supported_keep = [KeepType.DEFAULT.value, KeepType.FIRST.value, KeepType.LAST.value]
            if keep not in supported_keep:
                raise IllegalTransformerParameterException("Unsupported keep way {}, only {} are supported now!"
                                                            .format(keep, ", ".join(supported_keep)))
        return _param_check
