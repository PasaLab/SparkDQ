from sparkdq.analytics.Analyzer import ComplexAnalyzer
from sparkdq.analytics.CommonFunctions import metric_from_value, metric_from_empty, metric_from_failure
from sparkdq.analytics.metrics.KeyedViolationsMetric import KeyedViolationsMetric
from sparkdq.analytics.states.CFDConflicts import CFDConflicts
from sparkdq.analytics.Preconditions import has_column
from sparkdq.exceptions.AnalysisExceptions import AnalysisCalculationException, AnalysisException
from sparkdq.exceptions.CommonExceptions import NoSummaryException
from sparkdq.models.CFDSolver import CFDSolver
from sparkdq.models.CommonUtils import DEFAULT_INDEX_COL
from sparkdq.structures.Entity import Entity
from sparkdq.structures.Violation import Violation
from sparkdq.structures.Violations import Violations


class FunctionalDependency(ComplexAnalyzer):

    def __init__(self, cfd_or_cfds, index_col=DEFAULT_INDEX_COL):
        """
        Note:
        1. here instance is the list of cfds
        2. entity is uncertain, but mostly MultiColumn, so uniformly regarded as MultiColumn
        :param cfd_or_cfds: string or list of string
        """
        if not isinstance(cfd_or_cfds, list):
            cfd_or_cfds = [cfd_or_cfds]
        super(FunctionalDependency, self).__init__(FunctionalDependency.__name__, Entity.Multicolumn, cfd_or_cfds)
        self.index_col = index_col

    def __eq__(self, other):
        return (self.instance == other.instance) and (self.index_col == other.index_col)

    def __hash__(self):
        return hash((",".join(self.instance), self.index_col))

    def preconditions(self):
        return [has_column(self.index_col)]

    def additional_preconditions(self):
        pass

    def to_failure_metric(self, exception):
        cls = KeyedViolationsMetric
        return metric_from_failure(self, cls, AnalysisException.wrap_if_necessary(exception))

    def compute_state_from_data(self, data, num_rows=None, state_provider=None):
        cfd_resolver = CFDSolver(cfds=self.instance, indexCol=self.index_col)
        model = cfd_resolver.fit(data)
        if not model.hasSummary:
            raise NoSummaryException("No summary for FD analyzer {}!".format(self))
        summary = model.summary
        if not summary.success:
            raise AnalysisCalculationException(summary.message)
        violations = summary.violations

        if (violations is None) or len(violations) == 0:
            return None

        total_violations = []
        for cfd_vios in violations.split("#"):
            total_violations.append([[Violation(int(attr_vios.split(",")[0]), int(attr_vios.split(",")[1]))
                                     for attr_vios in pat_vios.split(";")]
                                     for pat_vios in cfd_vios.split("/")])
        return CFDConflicts(Violations(total_violations))

    def compute_metric_from_state(self, state):
        cls = KeyedViolationsMetric
        if state is None:
            return metric_from_empty(self, cls)
        return metric_from_value(self, cls, state.violations())

    def to_json(self):
        return {
            "AnalyzerName": FunctionalDependency.__name__,
            "CFDs": self.instance,
            "IndexCol": self.index_col
        }

    @staticmethod
    def from_json(d):
        return FunctionalDependency(d["CFDs"], d["IndexCol"])

