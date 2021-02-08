from abc import abstractmethod


class MetricsRepositoryLoader:

    @abstractmethod
    def with_tag_values(self, tag_values):
        pass

    @abstractmethod
    def for_analyzers(self, analyzers):
        pass

    @abstractmethod
    def after(self, date_time):
        pass

    @abstractmethod
    def before(self, date_time):
        pass

    @abstractmethod
    def get(self):
        pass
