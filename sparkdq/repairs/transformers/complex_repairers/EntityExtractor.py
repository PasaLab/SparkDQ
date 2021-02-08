from sparkdq.analytics.Preconditions import has_columns, has_column
from sparkdq.exceptions.CommonExceptions import NoSummaryException
from sparkdq.exceptions.TransformExceptions import TransformCalculationException
from sparkdq.models.CommonUtils import DEFAULT_INDEX_COL, TaskType
from sparkdq.models.entity.EntityResolution import EntityResolution
from sparkdq.repairs.transformers.complex_repairers.ComplexRepairer import ComplexRepairer


class EntityExtractor(ComplexRepairer):

    def __init__(self, column_or_columns, index_col=DEFAULT_INDEX_COL):
        if not isinstance(column_or_columns, list):
            column_or_columns = [column_or_columns]
        self.columns = column_or_columns
        self.index_col = index_col

    def preconditions(self):
        return [has_columns(self.columns), has_column(self.index_col)]

    def transform(self, data, state_provider=None, check=True):
        if check:
            for condition in self.preconditions():
                condition(data.schema)
        es = EntityResolution(columns=self.columns, indexCol=self.index_col, taskType=TaskType.REPAIR.value)
        model = es.fit(data)
        if not model.hasSummary:
            raise NoSummaryException("No summary for EntityExtractor!")
        summary = model.summary
        if not summary.success:
            raise TransformCalculationException(summary.message)
        return summary.targetData
