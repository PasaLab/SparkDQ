import json

from sparkdq.models.CommonUtils import DEFAULT_FEATURES_COL, DEFAULT_STANDARDIZED_FEATURES_COL, DEFAULT_PREDICTION_COL
from sparkdq.outliers.AutoEncoderOutlierSolver import DEFAULT_RECONSTRUCTED_FEATURES_COL, \
    DEFAULT_RECONSTRUCTION_ERROR_COL
from sparkdq.outliers.params.OutlierSolverParams import OutlierSolverParams
from sparkdq.outliers.OutlierSolver import OutlierSolver


class AutoEncoderParams(OutlierSolverParams):

    def __init__(self, features_col=DEFAULT_FEATURES_COL, standardized_features_col=DEFAULT_STANDARDIZED_FEATURES_COL,
                 reconstructed_features_col=DEFAULT_RECONSTRUCTED_FEATURES_COL,
                 reconstruction_error_col=DEFAULT_RECONSTRUCTION_ERROR_COL,
                 prediction_col=DEFAULT_PREDICTION_COL, input_size=3, hidden_size=3, batch_size=24, num_epoches=100,
                 method="quantile", contamination=0.05, relative_error=0.001, deviation=1.5):
        self.input_size = input_size
        self.hidden_size = hidden_size
        self.batch_size = batch_size
        self.num_epoches = num_epoches

        self.features_col = features_col
        self.standardized_features_col = standardized_features_col
        self.reconstructed_features_col = reconstructed_features_col
        self.reconstruction_error_col = reconstruction_error_col
        self.prediction_col = prediction_col

        self.method = method
        self.contamination = contamination
        self.relative_error = relative_error
        self.deviation = deviation

    def model(self):
        return OutlierSolver.AutoEncoder

    @staticmethod
    def from_json(json_str):
        d = json.loads(json_str)
        return AutoEncoderParams(
            input_size=d["input_size"],
            hidden_size=d["hidden_size"],
            batch_size=d["batch_size"],
            num_epoches=d["num_epoches"],
            features_col=d["features_col"],
            standardized_features_col=d["standardized_features_col"],
            reconstructed_features_col=d["reconstructed_features_col"],
            reconstruction_error_col=d["reconstruction_error_col"],
            prediction_col=d["prediction_col"],
            method=d["method"],
            contamination=d["contamination"],
            relative_error=d["relative_error"],
            deviation=d["deviation"]
        )
