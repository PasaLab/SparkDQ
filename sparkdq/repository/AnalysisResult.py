from sparkdq.analytics.runners.AnalyzerContext import AnalyzerContext
from sparkdq.repository.ResultKey import ResultKey


class AnalysisResult:
    """
    Analysis Result including customized result key and analysis context,
    Analysis Context including metrics of all analyzers
    """
    def __init__(self, result_key, analyzer_context):
        self.result_key = result_key
        self.analyzer_context = analyzer_context

    def to_json(self):
        return {
            "ResultKey": self.result_key.to_json(),
            "AnalysisContext": self.analyzer_context.to_json()
        }

    @staticmethod
    def from_json(d):
        return AnalysisResult(ResultKey.from_json(d["ResultKey"]), AnalyzerContext.from_json(d["AnalysisContext"]))
