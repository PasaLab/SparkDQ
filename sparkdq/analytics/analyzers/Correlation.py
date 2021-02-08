from sparkdq.analytics.Analyzer import StandardScanShareableAnalyzer
from sparkdq.analytics.catalyst.DQFunctions import stateful_correlation
from sparkdq.analytics.CommonFunctions import conditional_selection, if_no_nulls_in
from sparkdq.analytics.Preconditions import is_numeric
from sparkdq.analytics.states.CorrelationState import CorrelationState
from sparkdq.structures.Entity import Entity


class Correlation(StandardScanShareableAnalyzer):

    def __init__(self, column1, column2, where=None):
        super(Correlation, self).__init__(Correlation.__name__, Entity.Multicolumn, sorted([column1, column2]))
        self.where = where

    def __eq__(self, other):
        return (self.instance == self.instance) and (self.where == other.where)

    def __hash__(self):
        return hash((Correlation.__name__, ",".join(self.instance), self.where))

    def additional_preconditions(self):
        return [is_numeric(x) for x in self.instance]

    def aggregation_functions(self):
        return [stateful_correlation(conditional_selection(self.instance[0], self.where),
                                     conditional_selection(self.instance[1], self.where))]

    def fetch_state_from_aggregation_result(self, result, offset):
        if if_no_nulls_in(result, offset):
            row = result[offset]
            n = row[0]
            if n > 0.0:
                return CorrelationState(n, row[1], row[2], row[3], row[4], row[5])
        return None

    def to_json(self):
        return {
            "AnalyzerName": Correlation.__name__,
            "Columns": self.instance,
            "Where": self.where
        }

    @staticmethod
    def from_json(d):
        columns = d["Columns"]
        return Correlation(columns[0], columns[1], d["Where"])
