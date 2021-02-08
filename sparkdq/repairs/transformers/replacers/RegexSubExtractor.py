from sparkdq.repairs.transformers.replacers.Replacer import Replacer, SubReplacer

from pyspark.sql.functions import regexp_extract


class RegexSubExtractor(SubReplacer):

    def __init__(self, column, sub_pattern):
        super(RegexSubExtractor, self).__init__(column, "fixed", None)
        self.sub_pattern = sub_pattern

    def select_condition(self):
        # direct used in select or otherwise
        return regexp_extract(self.column, self.sub_pattern, 0)

    def merge(self, other):
        return regexp_extract(other, self.sub_pattern, 0)

    def preconditions(self):
        return [Replacer._str_param_check(self.way, self.value)] + self.common_preconditions()
