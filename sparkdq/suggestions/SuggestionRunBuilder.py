from sparkdq.profiling.Profiler import DEFAULT_CARDINALITY_THRESHOLD
from sparkdq.suggestions.SuggestionRunner import SuggestionRunner


class SuggestionRunBuilder:

    def __init__(self, data):
        self.data = data
        self.constraint_rules = []
        self.specified_columns = None
        self.low_cardinality_threshold = DEFAULT_CARDINALITY_THRESHOLD
        self.test_set_ratio = None
        self.random_seed = None

    def run(self):
        return SuggestionRunner.do_suggestion_run(
            self.data,
            self.constraint_rules,
            self.specified_columns,
            self.low_cardinality_threshold,
            self.test_set_ratio,
            self.random_seed)

    def add_constraint_rule(self, constraint_rule):
        self.constraint_rules.append(constraint_rule)
        return self

    def add_constraint_rules(self, constraint_rules):
        self.constraint_rules.extend(constraint_rules)
        return self

    def set_test_set_ratio(self, new_ratio):
        self.test_set_ratio = new_ratio
        return self

    def set_random_seed(self, new_seed):
        self.random_seed = new_seed
        return self

    def set_low_cardinality_threshold(self, new_threshold):
        self.low_cardinality_threshold = new_threshold
        return self

