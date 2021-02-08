import json

from sparkdq.models.CommonUtils import DEFAULT_FEATURES_COL, DEFAULT_CLUSTER_COL, DEFAULT_DISTANCE_COL, \
    DEFAULT_PREDICTION_COL, DEFAULT_NORMALIZED_FEATURES_COL
from sparkdq.outliers.OutlierSolver import OutlierSolver
from sparkdq.outliers.params.OutlierSolverParams import OutlierSolverParams


class KMeansParams(OutlierSolverParams):

    def __init__(self, features_col=DEFAULT_FEATURES_COL, cluster_col=DEFAULT_CLUSTER_COL,
                 distance_col=DEFAULT_DISTANCE_COL, prediction_col=DEFAULT_PREDICTION_COL, normalize=False,
                 normalized_features_col=DEFAULT_NORMALIZED_FEATURES_COL, k=2, max_iter=20, seed=None, min_cluster=1,
                 deviation=1.5):
        self.k = k
        self.max_iter = max_iter
        self.seed = seed

        self.features_col = features_col
        self.cluster_col = cluster_col
        self.distance_col = distance_col
        self.prediction_col = prediction_col
        self.normalize = normalize
        self.normalized_features_col = normalized_features_col

        self.min_cluster = min_cluster
        self.deviation = deviation

    def model(self):
        return OutlierSolver.KMeans

    @staticmethod
    def from_json(json_str):
        d = json.loads(json_str)
        return KMeansParams(
            k=d["k"],
            max_iter=d["max_iter"],
            seed=d["seed"],
            features_col=d["features_col"],
            cluster_col=d["cluster_col"],
            distance_col=d["distance_col"],
            prediction_col=d["prediction_col"],
            normalize=d["normalize"],
            normalized_features_col=d["normalized_features_col"],
            min_cluster=d["min_cluster"],
            deviation=d["deviation"]
        )
