from sparkdq.constraints.Constraints import Constraints
from sparkdq.suggestions.ConstraintSuggestion import ConstraintSuggestion
from sparkdq.suggestions.rules.ConstraintRule import ConstraintRule
from sparkdq.utils.Assertions import is_one


class CompleteRule(ConstraintRule):

    def is_verified(self, profile, num_records):
        if profile.completeness is not None:
            return profile.completeness == 1.0
        return False

    def generate_constraint_suggestion(self, profile, num_records):
        constraint = Constraints.complete_constraint(profile.column, is_one())
        description = "{} is not null".format(profile.column)
        return ConstraintSuggestion(constraint, profile.column, "Completeness: {}".format(profile.completeness),
                                    description, self, ".is_complete(\"{}\")".format(profile.column))
