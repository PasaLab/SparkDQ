from abc import abstractmethod


class Metric:
    """
    Abstract class for Metrics representing different result forms

    Args:w
        name: the name of the metric
        entity: the entity on which the metric acts
        instance: the specified entity
        value: the result value if successful else exception
        is_success: whether the metric is successful
    """
    def __init__(self, name, entity, instance, value, is_success):
        self.name = name
        self.entity = entity
        self.instance = instance
        self.value = value
        self.is_success = is_success

    def __str__(self):
        return "Name: {}, Entity: {}, Instance: {}, Value: {}, Is_success: {}"\
            .format(self.name, self.entity, self.instance, self.value, self.is_success)

    @abstractmethod
    def flatten(self):
        pass

    @abstractmethod
    def to_json(self):
        pass

    @staticmethod
    @abstractmethod
    def from_json(d):
        pass

    @abstractmethod
    def represent_value(self):
        pass
