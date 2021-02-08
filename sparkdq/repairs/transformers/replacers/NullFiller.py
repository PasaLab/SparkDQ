from pyspark.sql.functions import col

from sparkdq.repairs.transformers.replacers.Replacer import Replacer


class NullFiller(Replacer):
    """
    Handle null filling task for one column.
    If fill way is 'forward' or 'backward', schedule the task as Transformer, otherwise use 'fillna' of pyspark to fill
    uniformly.
    """
    def __init__(self, column, way, value=None):
        super(NullFiller, self).__init__(column, way, value)

    def select_condition(self):
        return col(self.column).isNull()

    def preconditions(self):
        return self.common_preconditions()
