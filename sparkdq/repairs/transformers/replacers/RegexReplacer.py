from sparkdq.repairs.transformers.replacers.Replacer import Replacer

from pyspark.sql.functions import col


class RegexReplacer(Replacer):

    def __init__(self, column, pattern, way, value=None, negative=False):
        super(RegexReplacer, self).__init__(column, way, value)
        self.pattern = pattern
        self.negative = negative

    def preconditions(self):
        return [Replacer._str_param_check(self.way, self.value)] + self.common_preconditions()

    def select_condition(self):
        condition = col(self.column).rlike(self.pattern)
        return condition if not self.negative else ~condition
