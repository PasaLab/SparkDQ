from pyspark.sql.functions import col

from sparkdq.analytics.Preconditions import has_column
from sparkdq.repairs.transformers.filters.Filter import Filter


class NullFilter(Filter):
    """
    Filter rows containing null in some column.
    """
    def __init__(self, column):
        self.column = column

    def filter_condition(self):
        return col(self.column).isNotNull()

    def preconditions(self):
        return [has_column(self.column)]
