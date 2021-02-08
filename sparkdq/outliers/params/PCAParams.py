import json

from sparkdq.models.CommonUtils import DEFAULT_FEATURES_COL, DEFAULT_STANDARDIZED_FEATURES_COL, DEFAULT_DISTANCE_COL, \
    DEFAULT_PREDICTION_COL
from sparkdq.outliers.params.OutlierSolverParams import OutlierSolverParams
from sparkdq.outliers.PCAOutlierSolver import DEFAULT_PCA_FEATURES_COL
from sparkdq.outliers.OutlierSolver import OutlierSolver


class PCAParams(OutlierSolverParams):

    def __init__(self, features_col=DEFAULT_FEATURES_COL, standardized_features_col=DEFAULT_STANDARDIZED_FEATURES_COL,
                 distance_col=DEFAULT_DISTANCE_COL, prediction_col=DEFAULT_PREDICTION_COL,
                 pca_features_col=DEFAULT_PCA_FEATURES_COL, k=3, method="quantile", contamination=0.05,
                 relative_error=0.001, deviation=1.5):
        self.k = k
        self.features_col = features_col
        self.standardized_features_col = standardized_features_col
        self.distance_col = distance_col
        self.prediction_col = prediction_col
        self.pca_features_col = pca_features_col

        self.method = method
        self.contamination = contamination
        self.relative_error = relative_error
        self.deviation = deviation

    def model(self):
        return OutlierSolver.PCA

    @staticmethod
    def from_json(json_str):
        d = json.loads(json_str)
        return PCAParams(
            k=d["k"],
            features_col=d["features_col"],
            standardized_features_col=d["standardized_features_col"],
            distance_col=d["distance_col"],
            prediction_col=d["prediction_col"],
            pca_features_col=d["pca_features_col"],
            method=d["method"],
            contamination=d["contamination"],
            relative_error=d["relative_error"],
            deviation=d["deviation"]
        )
