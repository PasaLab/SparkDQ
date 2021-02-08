from sparkdq.analytics.runners.AnalysisRunner import AnalysisRunner


class AnalysisRunBuilder:
    """
    Builder for running the analysis
    """
    def __init__(self, data):
        self.data = data
        self.analyzers = []

        self.metrics_repository = None
        self.result_key_for_reusing = None
        self.fail_if_reusing_key_missing = None
        self.result_key_for_saving = None

        self.output_path = None
        self.over_write = None

    def add_analyzer(self, analyzer):
        self.analyzers.append(analyzer)
        return self

    def add_analyzers(self, analyzers):
        self.analyzers.extend(analyzers)
        return self

    def set_repository(self, repository):
        self.metrics_repository = repository
        return self

    def set_result_key_for_reusing(self, result_key, fail_if_reusing_key_missing=False):
        self.result_key_for_reusing = result_key
        self.fail_if_reusing_key_missing = fail_if_reusing_key_missing
        return self

    def set_result_key_for_saving(self, result_key):
        self.result_key_for_saving = result_key
        return self

    def set_output_path(self, path):
        self.output_path = path
        return self

    def set_over_write(self, over_write):
        self.over_write = over_write
        return self

    def run(self):
        return AnalysisRunner.do_analysis_run(self.data, self.analyzers)
