from sparkdq.constraints.Constraints import Constraints, ConstrainableDataTypes
from sparkdq.structures.DataTypeInstances import DataTypeInstances
from sparkdq.suggestions.ConstraintSuggestion import ConstraintSuggestion
from sparkdq.suggestions.rules.ConstraintRule import ConstraintRule
from sparkdq.utils.Assertions import is_one


class RetainTypeRule(ConstraintRule):

    def is_verified(self, profile, num_records):
        types_for_rule = [DataTypeInstances.Integral, DataTypeInstances.Fractional, DataTypeInstances.Boolean]
        if (profile.data_type in types_for_rule) and profile.is_inferred_type:
            return True
        return False

    def generate_constraint_suggestion(self, profile, num_records):
        if profile.data_type is DataTypeInstances.Integral:
            data_type = ConstrainableDataTypes.Integral
        elif profile.data_type is DataTypeInstances.Fractional:
            data_type = ConstrainableDataTypes.Fractional
        else:
            data_type = ConstrainableDataTypes.Boolean
        constraint = Constraints.data_type_constraint(profile.column, data_type, assertion=is_one())
        description = "{} has type {}".format(profile.column, data_type)
        code = ".has_data_type(\"{}\", \"{}\")".format(profile.column, data_type.value)
        return ConstraintSuggestion(constraint, profile.column, "DataType: {}".format(profile.data_type), description,
                                    self, code)
