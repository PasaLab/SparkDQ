from abc import abstractmethod
import json


class OutlierSolverParams:

    @abstractmethod
    def model(self):
        pass

    def to_json(self):
        return json.dumps(self.__dict__)

    @staticmethod
    @abstractmethod
    def from_json(json_str):
        pass
