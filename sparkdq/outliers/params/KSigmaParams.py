import json

from sparkdq.outliers.params.OutlierSolverParams import OutlierSolverParams
from sparkdq.outliers.OutlierSolver import OutlierSolver


class KSigmaParams(OutlierSolverParams):

    def __init__(self, deviation=1.5):
        self.deviation = deviation

    def model(self):
        return OutlierSolver.kSigma

    @staticmethod
    def from_json(json_str):
        d = json.loads(json_str)
        return KSigmaParams(d["deviation"])
