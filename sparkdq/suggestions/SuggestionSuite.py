from sparkdq.suggestions.SuggestionRunBuilder import SuggestionRunBuilder
from sparkdq.suggestions.rules.ApproximatelyUniqueRule import ApproximatelyUniqueRule
from sparkdq.suggestions.rules.CategoricalRangeRule import CategoricalRangeRule
from sparkdq.suggestions.rules.CompleteRule import CompleteRule
from sparkdq.suggestions.rules.NonNegativeNumbersRule import NonNegativeNumbersRule
from sparkdq.suggestions.rules.RetainCompletenessRule import RetainCompletenessRule
from sparkdq.suggestions.rules.RetainTypeRule import RetainTypeRule


DEFAULT_RULES = [CompleteRule(), RetainCompletenessRule(), RetainTypeRule(), CategoricalRangeRule(),
                 NonNegativeNumbersRule(), ApproximatelyUniqueRule()]


class SuggestionSuite:

    @staticmethod
    def on_data(data):
        return SuggestionRunBuilder(data)
