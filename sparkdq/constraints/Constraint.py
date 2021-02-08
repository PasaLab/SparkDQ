from abc import abstractmethod

from sparkdq.structures.DataTypeInstances import DataTypeInstances


class Constraint:

    @abstractmethod
    def evaluate(self, analysis_result):
        pass

    @staticmethod
    def ratio_of_types(ignore_unknown, key_type):
        def _ratio_of_types(distribution):
            if ignore_unknown:
                count = distribution.values[key_type.name].absolute
                if count == 0:
                    return 0
                total_count = sum(map(lambda x: x.absolute, distribution.values.values()))
                null_count = distribution.values[DataTypeInstances.Unknown.name].absolute
                return count / (total_count - null_count)
            else:
                return distribution.values[key_type.name].ratio
        return _ratio_of_types
