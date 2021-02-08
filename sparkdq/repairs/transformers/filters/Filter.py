from abc import abstractmethod

from sparkdq.repairs.transformers.Transformer import Transformer


class Filter(Transformer):

    @abstractmethod
    def filter_condition(self):
        pass

    def transform(self, data, check=True):
        if check:
            for condition in self.preconditions():
                condition(data.schema)
        return data.filter(self.filter_condition())
