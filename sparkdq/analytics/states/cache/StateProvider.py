from abc import abstractmethod


class StateProvider:

    @abstractmethod
    def has_state(self, analyzer):
        pass

    @abstractmethod
    def load(self, analyzer):
        pass

    @abstractmethod
    def persist(self, analyzer, state):
        pass
