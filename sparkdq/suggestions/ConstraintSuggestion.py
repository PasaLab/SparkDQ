class ConstraintSuggestion:

    def __init__(self, constraint, column, current_value, description, rule, code):
        self.constraint = constraint
        self.column = column
        self.current_value = current_value
        self.description = description
        self.rule = rule
        self.code = code

    def __str__(self):
        return "Constraint: {}\nColumn: {}\nCurrentValue: {}\nDescription: {}\nRule: {}\nCode: {}".format(
            self.constraint,
            self.column,
            self.current_value,
            self.description,
            self.rule,
            self.code
        )
