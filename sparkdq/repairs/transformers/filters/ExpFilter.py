from pyspark.sql.functions import expr

from sparkdq.repairs.transformers.filters.Filter import Filter


class ExpFilter(Filter):

    def __init__(self, exp):
        self.exp = exp

    def filter_condition(self):
        return expr(self.exp)

    def preconditions(self):
        return []


class ExpDroper(ExpFilter):

    def __init__(self, exp):
        super(ExpDroper, self).__init__(exp)

    def filter_condition(self):
        return ~expr(self.exp)
