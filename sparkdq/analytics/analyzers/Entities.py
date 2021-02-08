from sparkdq.analytics.Analyzer import ComplexAnalyzer
from sparkdq.analytics.CommonFunctions import metric_from_empty, metric_from_failure, metric_from_value
from sparkdq.analytics.metrics.DoubleMetric import DoubleMetric
from sparkdq.analytics.Preconditions import has_column
from sparkdq.analytics.states.EntitiesState import EntitiesState
from sparkdq.exceptions.AnalysisExceptions import AnalysisCalculationException, AnalysisException
from sparkdq.exceptions.CommonExceptions import NoSummaryException
from sparkdq.models.CommonUtils import DEFAULT_INDEX_COL
from sparkdq.models.entity.EntityResolution import EntityResolution
from sparkdq.structures.Entity import Entity


class Entities(ComplexAnalyzer):

    def __init__(self, column_or_columns, index_col=DEFAULT_INDEX_COL):
        if not isinstance(column_or_columns, list):
            column_or_columns = [column_or_columns]
        column_or_columns = sorted(column_or_columns)
        entity = Entity.Multicolumn if len(column_or_columns) > 1 else Entity.Column
        super(Entities, self).__init__(Entities.__name__, entity, column_or_columns)
        self.index_col = index_col

    def __eq__(self, other):
        return (self.instance == other.instance) and (self.index_col == other.index_col)

    def __hash__(self):
        return hash((Entities.__name__, ",".join(self.instance), self.index_col))

    def additional_preconditions(self):
        return [has_column(self.index_col)]

    def to_failure_metric(self, exception):
        cls = DoubleMetric
        return metric_from_failure(self, cls, AnalysisException.wrap_if_necessary(exception))

    def compute_state_from_data(self, data, num_rows=None, state_provider=None):
        es = EntityResolution(columns=self.instance, indexCol=self.index_col)
        model = es.fit(data)
        if not model.hasSummary:
            raise NoSummaryException("No summary for analyzer {}!".format(self))
        summary = model.summary
        if not summary.success:
            raise AnalysisCalculationException(summary.message)
        return EntitiesState(summary.numOfEntities)

    def compute_metric_from_state(self, state):
        cls = DoubleMetric
        if state is None:
            return metric_from_empty(self, cls)
        return metric_from_value(self, cls, state.metric_value())

    def to_json(self):
        return {
            "AnalyzerName": Entities.__name__,
            "Columns": self.instance,
            "IndexCol": self.index_col
        }

    @staticmethod
    def from_json(d):
        return Entities(d["Columns"], d["IndexCol"])
