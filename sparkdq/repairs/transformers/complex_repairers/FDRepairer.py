from sparkdq.analytics.Preconditions import has_column
from sparkdq.exceptions.CommonExceptions import NoSummaryException, InconsistentParametersException
from sparkdq.exceptions.TransformExceptions import TransformCalculationException
from sparkdq.models.CFDSolver import CFDSolver
from sparkdq.models.CommonUtils import DEFAULT_INDEX_COL, TaskType
from sparkdq.repairs.transformers.complex_repairers.ComplexRepairer import ComplexRepairer


class FDRepairer(ComplexRepairer):

    def __init__(self, cfd_or_cfds, index_col=DEFAULT_INDEX_COL, priorities=[]):
        if not isinstance(cfd_or_cfds, list):
            cfd_or_cfds = [cfd_or_cfds]
        self.cfds = cfd_or_cfds
        self.index_col = index_col
        self.priorities = priorities

    def preconditions(self):
        return [has_column(self.index_col) + FDRepairer._param_check(self.priorities, self.cfds)]

    def transform(self, data, state_provider=None, check=True):
        if check:
            for condition in self.preconditions():
                condition(data.schema)
        cfd_resolver = CFDSolver(cfds=self.cfds, priorities=self.priorities, indexCol=self.index_col,
                                 taskType=TaskType.REPAIR.value)
        model = cfd_resolver.fit(data)
        if not model.hasSummary:
            raise NoSummaryException("No summary for FDRepairer!")
        summary = model.summary
        if not summary.success:
            raise TransformCalculationException(summary.message)
        return summary.targetData

    @staticmethod
    def _param_check(priorities, cfds):
        def _param_check(_):
            prio_len = len(priorities)
            if (prio_len != 0) and (len(cfds) != prio_len):
                raise InconsistentParametersException("Inconsistent number of cfds and priorities, there are {} cfds "
                                                      "but {} priority values!".format(len(cfds), prio_len))
        return _param_check
