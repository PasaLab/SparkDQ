import math

from sparkdq.constraints.Constraints import Constraints
from sparkdq.suggestions.ConstraintSuggestion import ConstraintSuggestion
from sparkdq.suggestions.rules.ConstraintRule import ConstraintRule
from sparkdq.utils.Assertions import is_one


class ApproximatelyUniqueRule(ConstraintRule):

    def is_verified(self, profile, num_records):
        if profile.approx_count_distinct and profile.completeness:
            approximate_distinctness = profile.approx_count_distinct / num_records
            beta = 1.0389

            relative_sd = 0.05
            m = 1 << math.ceil(2.0 * math.log(1.106 / relative_sd) / math.log(2.0))
            error_bound = beta / math.sqrt(m)
            return (profile.completeness == 1.0) and (math.fabs(1.0 - approximate_distinctness) <= error_bound)
        return False

    def generate_constraint_suggestion(self, profile, num_records):
        constraint = Constraints.uniqueness_constraint(profile.column, is_one())
        approximate_distinctness = profile.approx_count_distinct / num_records
        code = ".is_unique(\"{}\")".format(profile.column)
        return ConstraintSuggestion(constraint, profile.column,
                                    "ApproxDistinctness: {}".format(approximate_distinctness),
                                    "{} is unique".format(profile.column), self, code)
