from sparkdq.conf.Context import Context


class ConstraintSuggestionResult:

    def __init__(self, profiles, constraint_suggestions, verification_result):
        self.profiles = profiles
        self.constraint_suggestions = constraint_suggestions
        self.verification_result = verification_result

    def suggestion_df(self):
        constraints = list()
        for suggestion in self.constraint_suggestions:
            constraints.append([suggestion.column, suggestion.description, suggestion.code])
        return Context().spark.createDataFrame(constraints, ["column", "description", "code"])
