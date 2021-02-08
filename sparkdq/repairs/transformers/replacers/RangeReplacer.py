from pyspark.sql.functions import expr

from sparkdq.analytics.Preconditions import is_numeric
from sparkdq.repairs.transformers.replacers.Replacer import Replacer


class RangeReplacer(Replacer):

    def __init__(self, column, lower_bound, lower_closed, upper_bound, upper_closed, way, value=None):
        super(RangeReplacer, self).__init__(column, way, value)
        self.lower_bound = lower_bound
        self.lower_closed = lower_closed
        self.upper_bound = upper_bound
        self.upper_closed = upper_closed

    def select_condition(self):
        if (self.lower_bound is None) and (self.upper_bound is None):
            return None
        elif self.lower_bound is None:
            if self.upper_closed:
                return expr("{} <= {}".format(self.column, self.upper_bound))
            else:
                return expr("{} < {}".format(self.column, self.upper_bound))
        elif self.upper_bound is None:
            if self.lower_closed:
                return expr("{} >= {}".format(self.column, self.lower_bound))
            else:
                return expr("{} > {}".format(self.column, self.lower_bound))
        else:
            if self.lower_closed and self.upper_closed:
                return expr("{0} >= {1} and {0} <= {2}".format(self.column, self.lower_bound, self.upper_bound))
            elif self.lower_closed:
                return expr("{0} >= {1} and {0} < {2}".format(self.column, self.lower_bound, self.upper_bound))
            elif self.upper_closed:
                return expr("{0} > {1} and {0} <= {2}".format(self.column, self.lower_bound, self.upper_bound))
            else:
                return expr("{0} > {1} and {0} < {2}".format(self.column, self.lower_bound, self.upper_bound))

    def preconditions(self):
        return [is_numeric(self.column),
                RangeReplacer.param_check(self.lower_bound, self.lower_closed, self.upper_bound, self.upper_closed),
                Replacer._num_param_check(self.way, self.value)] + self.common_preconditions()

    @staticmethod
    def param_check(low_bound, lower_closed, upper_bound, upper_closed):
        def _param_check(_):
            if low_bound > upper_bound:
                raise Exception("Lower bound must be no greater than upper bound, "
                                "but low bound is {} and upper bound is {}!".format(low_bound, upper_bound))
            if (low_bound == upper_bound) and (low_bound is not None) and (not lower_closed or not upper_closed):
                raise Exception("If lower bound is equal to upper bound, two sides must be both closed!")
            if (low_bound is None) and (upper_bound is None):
                raise Exception("Meaningless number range, lower and upper bound are both endless!")
        return _param_check
