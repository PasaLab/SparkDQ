from abc import abstractmethod


class MetricsRepository:

    @abstractmethod
    def save(self, result_key, analyzer_context):
        pass

    @abstractmethod
    def load_by_key(self, result_key):
        pass

    @abstractmethod
    def load(self):
        pass
