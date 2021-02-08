import json

from sparkdq.models.CommonUtils import DEFAULT_FEATURES_COL, DEFAULT_CLUSTER_COL, DEFAULT_PREDICTION_COL, \
    DEFAULT_DISTANCE_COL
from sparkdq.outliers.GMMOutlierSolver import DEFAULT_PROBABILITY_COL
from sparkdq.outliers.params.OutlierSolverParams import OutlierSolverParams
from sparkdq.outliers.OutlierSolver import OutlierSolver


class GMMParams(OutlierSolverParams):

    def __init__(self, features_col=DEFAULT_FEATURES_COL, cluster_col=DEFAULT_CLUSTER_COL,
                 probability_col=DEFAULT_PROBABILITY_COL, distance_col=DEFAULT_DISTANCE_COL,
                 prediction_col=DEFAULT_PREDICTION_COL, k=2, max_iter=100, tol=0.01, seed=None, min_cluster=1,
                 deviation=1.5):
        self.k = k
        self.max_iter = max_iter
        self.seed = seed
        self.tol = tol
        self.features_col = features_col
        self.cluster_col = cluster_col
        self.probability_col = probability_col
        self.distance_col = distance_col
        self.prediction_col = prediction_col

        self.min_cluster = min_cluster
        self.deviation = deviation

    def model(self):
        return OutlierSolver.GMM

    @staticmethod
    def from_json(json_str):
        d = json.loads(json_str)
        return GMMParams(
            k=d["k"],
            max_iter=d["max_iter"],
            tol=d["tol"],
            seed=d["seed"],
            features_col=d["features_col"],
            cluster_col=d["cluster_col"],
            probability_col=d["probability_col"],
            distance_col=d["distance_col"],
            prediction_col=d["prediction_col"],
            min_cluster=d["min_cluster"],
            deviation=d["deviation"]
        )
