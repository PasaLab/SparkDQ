import math

from sparkdq.constraints.Constraints import Constraints
from sparkdq.suggestions.ConstraintSuggestion import ConstraintSuggestion
from sparkdq.suggestions.rules.ConstraintRule import ConstraintRule
from sparkdq.utils.Assertions import greater_than_or_equal_to


class RetainCompletenessRule(ConstraintRule):

    def is_verified(self, profile, num_records):
        if profile.completeness is not None:
            return (profile.completeness > 0.2) and (profile.completeness < 1.0)
        return False

    def generate_constraint_suggestion(self, profile, num_records):
        p = profile.completeness
        n = num_records
        z = 1.96
        estimated_completeness = float("%.2f" % (p - z * math.sqrt(p * (1 - p) / n)))
        constraint = Constraints.complete_constraint(profile.column, greater_than_or_equal_to(estimated_completeness))
        missing_percent = int((1 - estimated_completeness) * 100)
        description = "{} has less than {}% missing values".format(profile.column, missing_percent)
        current_value = "Completeness: {}".format(profile.completeness)
        code = ".has_completeness(\"{}\", Assertions.greater_than_or_equal_to({}))".format(profile.column,
                                                                                           estimated_completeness)
        return ConstraintSuggestion(constraint, profile.column, current_value, description, self, code)
