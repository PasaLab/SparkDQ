import json

from sparkdq.outliers.params.OutlierSolverParams import OutlierSolverParams
from sparkdq.outliers.OutlierSolver import OutlierSolver


class SMAParams(OutlierSolverParams):

    def __init__(self, deviation=1.5, radius=5):
        self.deviation = deviation
        self.radius = radius

    def model(self):
        return OutlierSolver.MA

    @staticmethod
    def from_json(json_str):
        d = json.loads(json_str)
        return SMAParams(
            deviation=d["deviation"],
            radius=d["radius"]
        )
