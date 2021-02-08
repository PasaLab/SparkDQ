from sparkdq.analytics.Analyzer import StandardScanShareableAnalyzer
from sparkdq.analytics.catalyst.DQFunctions import stateful_mode
from sparkdq.analytics.CommonFunctions import if_no_nulls_in, conditional_selection
from sparkdq.analytics.states.ModeState import ModeState
from sparkdq.structures.Entity import Entity


class Mode(StandardScanShareableAnalyzer):

    def __init__(self, column, where=None):
        super(Mode, self).__init__(Mode.__name__, Entity.Column, column)
        self.where = where

    def __eq__(self, other):
        return (self.instance == other.instance) and (self.where == other.where)

    def __hash__(self):
        return hash((Mode.__name__, self.instance, self.where))

    def additional_preconditions(self):
        return []

    def aggregation_functions(self):
        return [stateful_mode(conditional_selection(self.instance, self.where))]

    def fetch_state_from_aggregation_result(self, result, offset):
        if if_no_nulls_in(result, offset):
            return ModeState(result[offset])
        return None

    def to_json(self):
        return {
            "AnalyzerName": Mode.__name__,
            "Column": self.instance,
            "Where": self.where
        }

    @staticmethod
    def from_json(d):
        return Mode(d["Column"], d["Where"])
