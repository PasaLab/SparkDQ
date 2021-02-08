from sparkdq.checks.Check import Check
from sparkdq.detections.DetectionSuite import DetectionSuite
from sparkdq.profiling.ProfilerRunner import ProfilerRunner
from sparkdq.suggestions.ConstraintSuggestionResult import ConstraintSuggestionResult


class SuggestionRunner:

    @staticmethod
    def do_suggestion_run(data, constraint_rules, specified_columns, low_cardinality_threshold, test_set_ratio,
                          random_seed):
        assert len(constraint_rules) > 0, "There should be at least one rule!"
        if test_set_ratio is not None:
            assert (test_set_ratio > 0) and (test_set_ratio < 1), "Test data ratio must be in (0, 1)"
        if specified_columns is None:
            specified_columns = data.schema.names
        train_set, test_set = SuggestionRunner.split_train_and_test(data, test_set_ratio, random_seed)

        profiles, constraint_suggestions = SuggestionRunner.profile_and_suggest(train_set, constraint_rules,
                                                                                specified_columns,
                                                                                low_cardinality_threshold)

        verification_result = None
        if test_set is not None:
            verification_result = SuggestionRunner.verify_constraint_suggestions(test_set, constraint_suggestions)
        return ConstraintSuggestionResult(profiles, constraint_suggestions, verification_result)

    @staticmethod
    def split_train_and_test(data, test_set_ratio, random_seed):
        if (test_set_ratio is None) or (random_seed is None):
            return data, None
        train_set_ratio = 1 - test_set_ratio
        splits = data.randomSplit([train_set_ratio, test_set_ratio], random_seed)
        return splits[0], splits[1]

    @staticmethod
    def profile_and_suggest(train_set, constraint_rules, specified_columns, low_cardinality_threshold):
        profiles = ProfilerRunner\
            .on_data(train_set)\
            .set_specified_columns(specified_columns)\
            .set_low_cardinality_threshold(low_cardinality_threshold)\
            .run()

        suggestions = SuggestionRunner.apply_rules(constraint_rules, profiles, specified_columns)
        return profiles, suggestions

    @staticmethod
    def apply_rules(constraint_rules, profiles, specified_columns):
        num_records = profiles.num_records
        constraint_suggestions = []
        for column in specified_columns:
            profile = profiles.profiles[column]
            for rule in constraint_rules:
                verified = rule.is_verified(profile, num_records)
                if verified:
                    constraint_suggestions.append(rule.generate_constraint_suggestion(profile, num_records))
        return constraint_suggestions

    @staticmethod
    def verify_constraint_suggestions(test_set, constraint_suggestions):
        constraints = []
        for suggestion in constraint_suggestions:
            constraints.append(suggestion.constraint)
        check = Check("Suggested constraints verification", constraints)
        return DetectionSuite().on_data(test_set).add_check(check).run()
