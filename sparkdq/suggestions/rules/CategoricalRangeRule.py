from sparkdq.constraints.Constraints import Constraints
from sparkdq.structures.DataTypeInstances import DataTypeInstances
from sparkdq.suggestions.ConstraintSuggestion import ConstraintSuggestion
from sparkdq.suggestions.rules.ConstraintRule import ConstraintRule
from sparkdq.utils.Assertions import is_one


class CategoricalRangeRule(ConstraintRule):

    def is_verified(self, profile, num_records):
        if (profile.histogram is not None) and (profile.data_type == DataTypeInstances.String):
            entries = profile.histogram.values
            num_unique_elements = 0
            for e in entries.values():
                if e.absolute == 1:
                    num_unique_elements += 1
            unique_ratio = num_unique_elements / len(entries)
            return unique_ratio <= 0.1
        return False

    def generate_constraint_suggestion(self, profile, num_records):
        values = profile.histogram.values.keys()
        categorical_values = "'{}'".format("', '".join(values))
        description = "{} has value range {}".format(profile.column, categorical_values)
        column_condition = "`{}` in {}".format(profile.column, categorical_values)
        constraint = Constraints.compliance_constraint(column_condition, is_one())
        code = ".is_contained_in(\"{}\", {})".format(profile.column, "[{}]".format(", ".join(values)))
        return ConstraintSuggestion(constraint, profile.column, "Compliance: 1", description, self, code)


