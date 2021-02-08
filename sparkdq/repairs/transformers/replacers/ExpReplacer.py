from pyspark.sql.functions import expr

from sparkdq.repairs.transformers.replacers.Replacer import Replacer


class ExpReplacer(Replacer):

    def __init__(self, column, exp, way, value=None):
        super(ExpReplacer, self).__init__(column, way, value)
        self.exp = exp

    def select_condition(self):
        return expr(self.exp)

    def preconditions(self):
        return self.common_preconditions()
