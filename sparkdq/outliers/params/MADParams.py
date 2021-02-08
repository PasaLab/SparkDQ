import json

from sparkdq.outliers.OutlierSolver import OutlierSolver
from sparkdq.outliers.params.OutlierSolverParams import OutlierSolverParams


class MADParams(OutlierSolverParams):

    def __init__(self, deviation=1.5, relative_error=0.001):
        self.deviation = deviation
        self.relative_error = relative_error

    def model(self):
        return OutlierSolver.MAD

    @staticmethod
    def from_json(json_str):
        d = json.loads(json_str)
        return MADParams(
            deviation=d["deviation"],
            relative_error=d["relative_error"]
        )
