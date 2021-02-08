from sparkdq.analytics.runners.AnalysisRunner import AnalysisRunner
from sparkdq.detections.DetectionResult import DetectionResult
from sparkdq.detections.DetectionRunBuilder import DetectionRunBuilder


class DetectionSuite:
    """
    Detection suite for checking data quality
    """
    @staticmethod
    def on_data(data):
        return DetectionRunBuilder(data)

    @staticmethod
    def do_detection_run(data, checks):
        """
        Fetch corresponding analyzers of each Check and merge them
        Do analysis and evaluate the result
        :param data: source data of type DataFrame
        :param checks: all the Checks, each Check contains a group of analyzers
        :return: detection result
        """
        analyzers = []
        for check in checks:
            analyzers.extend(check.required_analyzers())
        analysis_result = AnalysisRunner.do_analysis_run(data, analyzers)
        detection_result = DetectionSuite.evaluate(checks, analysis_result)
        return detection_result

    @staticmethod
    def evaluate(checks, analyzer_context):
        """
        Evaluate detection result for all checks
        :param checks: all the checks
        :param analyzer_context: analysis context containing all the metrics
        :return: detection result
        """
        check_results = {}
        for check in checks:
            check_results[check] = check.evaluate(analyzer_context)
        detection_status = max([result.status for result in check_results.values()])
        return DetectionResult(detection_status, check_results, analyzer_context.metric_map)
