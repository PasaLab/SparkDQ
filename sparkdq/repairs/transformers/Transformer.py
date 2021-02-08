from abc import abstractmethod


class Transformer:

    @abstractmethod
    def preconditions(self):
        pass

    @abstractmethod
    def transform(self, data, check=True):
        pass
