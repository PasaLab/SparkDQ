import json

from sparkdq.utils.ClassHelper import get_class_by_name


class AnalyzerContext:
    """
    AnalyzerContext holds and manages metrics of analyzers
    """
    def __init__(self, metric_map):
        self.metric_map = metric_map

    def metric(self, analyzer):
        return self.metric_map[analyzer]

    def metrics(self):
        return self.metric_map.values()

    @staticmethod
    def empty():
        return AnalyzerContext({})

    def add(self, other_metric_map):
        """
        Add another metric map to this context
        :param other_metric_map: another metric map
        :return context with another map of metrics
        """
        self.metric_map.update(other_metric_map)
        return self

    def merge(self, other_analyzer_context):
        """
        Merge another analyzer context into this context
        :param other_analyzer_context: another context
        :return merged new context
        """
        self.metric_map.update(other_analyzer_context.metric_map)
        return self

    def to_json(self):
        metric_array = []
        for analyzer, metric in self.metric_map.items():
            metric_array.append({'Analyzer': analyzer.to_json(),
                                 'Metric': metric.to_json()})
        return {
            "MetricMap": metric_array
        }

    @staticmethod
    def from_json(d):
        metrics = d["MetricMap"]
        metric_map = {}
        for analyzer_metric in metrics:
            analyzer = analyzer_metric["Analyzer"]
            analyzer_name = analyzer["AnalyzerName"]
            analyzer_class = get_class_by_name("sparkdq.analytics.analyzers.{}".format(analyzer_name), analyzer_name)
            analyzer = analyzer_class.from_json(analyzer)

            metric = analyzer_metric["Metric"]
            metric_name = metric["MetricName"]
            metric_class = get_class_by_name("sparkdq.analytics.metrics.{}".format(metric_name), metric_name)
            metric = metric_class.from_json(metric)
            metric_map[analyzer] = metric
        return AnalyzerContext(metric_map)

    @staticmethod
    def success_metrics_as_json(analyzer_context, limited_analyzers=None):
        metrics = AnalyzerContext.get_simplified_successful_metrics(analyzer_context, limited_analyzers)
        result = [{"entity": m.entity.name, "instance": m.instance, "name": m.name, "value": m.value} for m in metrics]
        return json.dumps(result)

    @staticmethod
    def get_simplified_successful_metrics(analyzer_context, limited_analyzers=None):
        metrics_list = []
        for analyzer, metric in analyzer_context.metric_map.items():
            if (limited_analyzers is None) or (analyzer in limited_analyzers):
                if metric.is_success:
                    metrics_list.extend(metric.flatten())
        return metrics_list
