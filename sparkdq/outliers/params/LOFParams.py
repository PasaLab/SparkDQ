import json

from sparkdq.models.CommonUtils import DEFAULT_FEATURES_COL, DEFAULT_PREDICTION_COL, DEFAULT_ANOMALY_SCORE_COL
from sparkdq.outliers.params.OutlierSolverParams import OutlierSolverParams
from sparkdq.outliers.OutlierSolver import OutlierSolver


class LOFParams(OutlierSolverParams):

    def __init__(self, features_col=DEFAULT_FEATURES_COL, lof_col=DEFAULT_ANOMALY_SCORE_COL,
                 prediction_col=DEFAULT_PREDICTION_COL, min_pts=5, dist_type="euclidean", contamination=0.05,
                 relative_error=0.001):
        self.features_col = features_col
        self.lof_col = lof_col
        self.prediction_col = prediction_col
        self.min_pts = min_pts
        self.dist_type = dist_type

        self.contamination = contamination
        self.relative_error = relative_error

    def model(self):
        return OutlierSolver.LOF

    @staticmethod
    def from_json(json_str):
        d = json.loads(json_str)
        return LOFParams(
            features_col=d["features_col"],
            lof_col=d["lof_col"],
            prediction_col=d["prediction_col"],
            min_pts=d["min_pts"],
            dist_type=d["dist_type"],
            contamination=d["contamination"],
            relative_error=d["relative_error"]
        )
