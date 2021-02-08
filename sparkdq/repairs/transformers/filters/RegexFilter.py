from pyspark.sql.functions import col

from sparkdq.analytics.Preconditions import has_column
from sparkdq.repairs.transformers.filters.Filter import Filter


class RegexFilter(Filter):

    def __init__(self, column, pattern):
        self.column = column
        self.pattern = pattern

    def filter_condition(self):
        return col(self.column).rlike(self.pattern)

    def preconditions(self):
        return [has_column(self.column)]


class RegexDroper(RegexFilter):

    def __init__(self, column, pattern):
        super(RegexDroper, self).__init__(column, pattern)

    def filter_condition(self):
        return ~super(RegexDroper, self).filter_condition()
