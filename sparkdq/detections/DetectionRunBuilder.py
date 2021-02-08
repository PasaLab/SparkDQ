class DetectionRunBuilder:

    def __init__(self, data, checks=None, required_analyzers=None, metrics_repository=None):
        self.data = data
        if checks is None:
            self.checks = []
        else:
            self.checks = checks
        self.required_analyzers = required_analyzers
        self.metrics_repository = metrics_repository

    def copy(self, detection_builder):
        self.checks = list(detection_builder.checks)
        self.data = detection_builder.data
        self.required_analyzers = detection_builder.required_analyzers
        self.metrics_repository = detection_builder.metricsRepository
        return self

    def add_check(self, check):
        self.checks.append(check)
        return self

    def use_repository(self, metrics_repository):
        from sparkdq.detections.DetectionRunBuilderWithRepository import DetectionRunBuilderWithRepository
        return DetectionRunBuilderWithRepository(self, metrics_repository)

    def run(self):
        from sparkdq.detections.DetectionSuite import DetectionSuite
        return DetectionSuite.do_detection_run(self.data, self.checks)
