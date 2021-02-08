from sparkdq.analytics.metrics.DoubleMetric import DoubleMetric
from sparkdq.analytics.metrics.Metric import Metric
from sparkdq.structures.Entity import Entity
from sparkdq.structures.Violations import Violations


class KeyedViolationsMetric(Metric):

    def __init__(self, name, entity, instance, total_violations, is_success):
        """
        :param instance:            list of CFDs
        :param total_violations:    list of Violations for all CFDs
        """
        super(KeyedViolationsMetric, self).__init__(name, entity, instance, total_violations, is_success)

    def flatten(self):
        details = []
        for i in range(len(self.value)):
            cfd = self.instance[i]
            cfd_vios = self.value[i].violations
            for j in range(len(cfd_vios)):
                pat_vios = cfd_vios[j]
                for k in range(len(pat_vios)):
                    rhs_vios = pat_vios[k]
                    details.append(DoubleMetric(self.name, self.entity, "{}.{}.{}.constVio".format(cfd, j, k),
                                                rhs_vios.const_vio, True))
                    details.append(DoubleMetric(self.name, self.entity, "{}.{}.{}.varVio".format(cfd, j, k),
                                                rhs_vios.var_vio, True))
        return details

    def to_json(self):
        if not self.is_success:
            raise Exception("Unable to serialize failed metrics.")
        return {
            "MetricName": KeyedViolationsMetric.__name__,
            "Entity": self.entity,
            "Instance": self.instance,
            "Name": self.name,
            "TotalViolations": self.value.to_json()
        }

    @staticmethod
    def from_json(d):
        total_violations = Violations.from_json(d["TotalViolations"])
        return KeyedViolationsMetric(d["Name"], Entity(d["Entity"]), d["Instance"], total_violations, True)

    def represent_value(self):
        return str(self.value.total_vios())
