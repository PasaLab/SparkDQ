from sparkdq.repairs.transformers.replacers.Replacer import Replacer, SubReplacer

from pyspark.sql.functions import regexp_replace


class RegexSubReplacer(SubReplacer):

    def __init__(self, column, sub_pattern, value):
        super(RegexSubReplacer, self).__init__(column, "fixed", value)
        self.sub_pattern = sub_pattern

    def select_condition(self):
        # direct used in select or otherwise
        return regexp_replace(self.column, self.sub_pattern, self.value)

    def merge(self, other):
        return regexp_replace(other, self.sub_pattern, self.value)

    def preconditions(self):
        return [Replacer._str_param_check(self.way, self.value)] + self.common_preconditions()
