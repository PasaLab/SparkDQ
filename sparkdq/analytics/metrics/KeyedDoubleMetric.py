from sparkdq.analytics.metrics.DoubleMetric import DoubleMetric
from sparkdq.analytics.metrics.Metric import Metric
from sparkdq.structures.Entity import Entity


class KeyedDoubleMetric(Metric):

    def __init__(self, name, entity, instance, dictionary, is_success):
        super(KeyedDoubleMetric, self).__init__(name, entity, instance, dictionary, is_success)

    def flatten(self):
        return list(map(lambda t: DoubleMetric("{}.{}".format(self.name, t[0]), self.entity, self.instance, t[1], True),
                    self.value.items()))

    def to_json(self):
        if not self.is_success:
            raise Exception("Unable to serialize failed metrics.")
        return {
            "MetricName": KeyedDoubleMetric.__name__,
            "Entity": self.entity,
            "Instance": self.instance,
            "Name": self.name,
            "Dictionary": self.value
        }

    @staticmethod
    def from_json(d):
        return KeyedDoubleMetric(d["Name"], Entity(d["Entity"]), d["Instance"], d["Dictionary"], True)

    def represent_value(self):
        return str(self.value)
