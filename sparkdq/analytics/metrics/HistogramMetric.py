from sparkdq.analytics.metrics.DoubleMetric import DoubleMetric
from sparkdq.analytics.metrics.Metric import Metric
from sparkdq.structures.Distribution import Distribution
from sparkdq.structures.Entity import Entity


class HistogramMetric(Metric):
    """
    Histogram metric, value is of type Distribution
    """
    def __init__(self, name, entity, instance, distribution, is_success):
        super(HistogramMetric, self).__init__(name, entity, instance, distribution, is_success)

    def flatten(self):
        num_of_bins = [DoubleMetric("{}.bins".format(self.name), self.entity, self.instance, self.value.number_of_bins,
                                    True)]
        details = []
        for key, value in self.value.values.items():
            details.append(DoubleMetric("{}.abs.{}".format(self.name, key), self.entity, self.instance, value.absolute,
                                        True))
            details.append(DoubleMetric("{}.ratio.{}".format(self.name, key), self.entity, self.instance, value.ratio,
                                        True))
        return num_of_bins + details

    def to_json(self):
        if not self.is_success:
            raise Exception("Unable to serialize failed metrics.")
        return {
            "MetricName": HistogramMetric.__name__,
            "Entity": self.entity,
            "Instance": self.instance,
            "Name": self.name,
            "Distribution": self.value.to_json()
        }

    @staticmethod
    def from_json(d):
        distribution = Distribution.from_json(d["Distribution"])
        return HistogramMetric(d["Name"], Entity(d["Entity"]), d["Instance"], distribution, True)

    def represent_value(self):
        return str(self.value)
