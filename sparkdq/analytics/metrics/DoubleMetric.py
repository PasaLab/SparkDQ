from sparkdq.analytics.metrics.Metric import Metric
from sparkdq.exceptions.AnalysisExceptions import AnalysisException
from sparkdq.structures.Entity import Entity


class DoubleMetric(Metric):

    def __init__(self, name, entity, instance, value, is_success):
        if is_success:
            try:
                value = float(value)
            except TypeError as e:
                value = AnalysisException.wrap_if_necessary(e)
        super(DoubleMetric, self).__init__(name, entity, instance, value, is_success)

    def flatten(self):
        return [self]

    def to_json(self):
        if not self.is_success:
            raise Exception("Unable to serialize failed metrics.")
        return {
            "MetricName": DoubleMetric.__name__,
            "Entity": self.entity,
            "Instance": self.instance,
            "Name": self.name,
            "Value": self.value
        }

    @staticmethod
    def from_json(d):
        return DoubleMetric(d["Name"], Entity(d["Entity"]), d["Instance"], d["Value"], True)

    def represent_value(self):
        return str(self.value)
