from sparkdq.analytics.Preconditions import has_column, has_columns
from sparkdq.exceptions.CommonExceptions import NoSummaryException
from sparkdq.exceptions.TransformExceptions import TransformCalculationException
from sparkdq.models.BayesianImputor import BayesianImputor
from sparkdq.models.CommonUtils import DEFAULT_INDEX_COL
from sparkdq.repairs.transformers.complex_repairers.ComplexRepairer import ComplexRepairer


class BayesianNullFiller(ComplexRepairer):

    def __init__(self, column, dependent_column_or_columns, allowed_values=[], index_col=DEFAULT_INDEX_COL):
        self.column = column
        if not isinstance(dependent_column_or_columns, list):
            dependent_column_or_columns = [dependent_column_or_columns]
        self.dependent_columns = dependent_column_or_columns
        self.allowed_values = allowed_values
        self.index_col = index_col

    def preconditions(self):
        return [has_column(self.column), has_columns(self.dependent_columns)]

    def transform(self, data, state_provider=None, check=True):
        if check:
            for condition in self.preconditions():
                condition(data.schema)
        bi = BayesianImputor(indexCol=self.index_col, dependentColumns=self.dependent_columns,
                             targetCol=self.column, allowedValues=self.allowed_values)
        model = bi.fit(data)
        if not model.hasSummary:
            raise NoSummaryException("No summary for BayesianNullFiller!")
        summary = model.summary
        if not summary.success:
            raise TransformCalculationException(summary.message)
        return summary.imputedData
