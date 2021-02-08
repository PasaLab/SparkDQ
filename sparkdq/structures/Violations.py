from sparkdq.structures.Violation import Violation


class Violations:

    def __init__(self, total_violations):
        """
        :param total_violations:  violations for all CFDs
        """
        self.total_violations = total_violations

    def total_vios(self):
        total = 0
        for cfd_vios in self.total_violations:
            for pat_vios in cfd_vios:
                for attr_vios in pat_vios:
                    total += attr_vios.const_vio + attr_vios.var_vio
        return total

    def to_json(self):
        return {
            "TotalViolations": [[[rhsVios.to_json() for rhsVios in patVios] for patVios in cfdVios]
                                for cfdVios in self.total_violations]
        }

    @staticmethod
    def from_json(d):
        violations = [[[Violation.from_json(rhsVios) for rhsVios in patVios] for patVios in cfdVios]
                      for cfdVios in d["TotalViolations"]]
        return Violations(violations)
