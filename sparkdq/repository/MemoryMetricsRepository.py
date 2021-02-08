from sparkdq.analytics.runners.AnalyzerContext import AnalyzerContext
from sparkdq.repository.AnalysisResult import AnalysisResult
from sparkdq.repository.MemoryMetricsRepositoryLoader import MemoryMetricsRepositoryLoader
from sparkdq.repository.MetricsRepository import MetricsRepository

analysis_results_repository = {}


class MemoryMetricsRepository(MetricsRepository):

    def save(self, result_key, analyzer_context):
        successful_metrics = {k: v for k, v in analyzer_context.metric_map.items() if v.is_success}
        analyzer_context_with_successful_metrics = AnalyzerContext(successful_metrics)
        analysis_results_repository[result_key] = AnalysisResult(result_key, analyzer_context_with_successful_metrics)

    def load_by_key(self, result_key):
        if result_key in analysis_results_repository:
            return analysis_results_repository[result_key].analyzer_context
        return AnalyzerContext.empty()

    def load(self):
        return MemoryMetricsRepositoryLoader(analysis_results_repository)
