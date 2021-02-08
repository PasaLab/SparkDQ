from sparkdq.detections.DetectionRunBuilder import DetectionRunBuilder


class DetectionRunBuilderWithRepository(DetectionRunBuilder):

    def __init__(self, detection_run_builder, using_metrics_repository):
        super(DetectionRunBuilderWithRepository, self).__init__(detection_run_builder)
        self.metrics_repository = using_metrics_repository
