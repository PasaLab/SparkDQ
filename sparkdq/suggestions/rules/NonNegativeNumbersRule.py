from sparkdq.checks.Check import is_one
from sparkdq.constraints.Constraints import Constraints
from sparkdq.profiling.Profiles import NumericProfile
from sparkdq.suggestions.ConstraintSuggestion import ConstraintSuggestion
from sparkdq.suggestions.rules.ConstraintRule import ConstraintRule


class NonNegativeNumbersRule(ConstraintRule):

    def is_verified(self, profile, num_records):
        if isinstance(profile, NumericProfile) and (profile.minimum is not None):
            return profile.minimum >= 0.0
        return False

    def generate_constraint_suggestion(self, profile, num_records):
        description = "{} has no negative values".format(profile.column)
        constraint = Constraints.compliance_constraint(description, "{} >= 0".format(profile.column), is_one())

        if isinstance(profile, NumericProfile):
            minimum = str(profile.minimum)
        else:
            minimum = "Error while computing minimum"
        value = "Minimum: {}".format(minimum)
        code = ".is_non_negative(\"{}\")".format(profile.column)
        return ConstraintSuggestion(constraint, profile.column, value, description, self, code)
