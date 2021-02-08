import json

from sparkdq.models.CommonUtils import DEFAULT_FEATURES_COL, DEFAULT_ANOMALY_SCORE_COL, DEFAULT_PREDICTION_COL
from sparkdq.outliers.params.OutlierSolverParams import OutlierSolverParams
from sparkdq.outliers.OutlierSolver import OutlierSolver


class IForestParams(OutlierSolverParams):

    def __init__(self, features_col=DEFAULT_FEATURES_COL, anomaly_score_col=DEFAULT_ANOMALY_SCORE_COL,
                 prediction_col=DEFAULT_PREDICTION_COL, num_trees=100, max_samples=1.0, max_features=1.0, max_depth=10,
                 bootstrap=False, seed=24, contamination=0.05, relative_error=0.001):
        self.num_trees = num_trees
        self.max_samples = max_samples
        self.max_features = max_features
        self.max_depth = max_depth
        self.bootstrap = bootstrap
        self.seed = seed

        self.features_col = features_col
        self.anomaly_score_col = anomaly_score_col
        self.prediction_col = prediction_col

        self.contamination = contamination
        self.relative_error = relative_error

    def model(self):
        return OutlierSolver.IForest

    @staticmethod
    def from_json(json_str):
        d = json.loads(json_str)
        return IForestParams(
            num_trees=d["num_trees"],
            max_samples=d["max_samples"],
            max_features=d["max_features"],
            max_depth=d["max_depth"],
            bootstrap=d["bootstrap"],
            seed=d["seed"],
            features_col=d["features_col"],
            anomaly_score_col=d["anomaly_score_col"],
            prediction_col=d["prediction_col"],
            contamination=d["contamination"],
            relative_error=d["relative_error"]
        )
