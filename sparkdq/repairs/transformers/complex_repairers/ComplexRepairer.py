from abc import abstractmethod

from sparkdq.repairs.transformers.Transformer import Transformer


class ComplexRepairer(Transformer):

    @abstractmethod
    def transform(self, data, state_provider=None, check=True):
        pass
