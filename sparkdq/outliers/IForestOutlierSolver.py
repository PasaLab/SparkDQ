from pyspark.sql.functions import when, expr, sum as _sum, col

from sparkdq.models.CommonUtils import DEFAULT_FEATURES_COL, DEFAULT_ANOMALY_SCORE_COL, DEFAULT_PREDICTION_COL
from sparkdq.models.IForest import IForest


class IForestOutlierSolver:
    """
    Isolation forest outlier solver
    """
    def __init__(self, num_trees=100, max_samples=1.0, max_features=1.0, max_depth=10, bootstrap=False, seed=24,
                 features_col=DEFAULT_FEATURES_COL, anomaly_score_col=DEFAULT_ANOMALY_SCORE_COL,
                 prediction_col=DEFAULT_PREDICTION_COL):
        self._num_trees = num_trees
        self._max_samples = max_samples
        self._max_features = max_features
        self._max_depth = max_depth
        self._bootstrap = bootstrap
        self._seed = seed
        self._features_col = features_col
        self._anomaly_score_col = anomaly_score_col
        self._prediction_col = prediction_col
        self._if = None
        self._num_rows = None
        self._df = None
        self._columns = None
        self._target_columns = None
        self._model = None

    def set_params(self, num_trees=100, max_samples=1.0, max_features=1.0, max_depth=10, bootstrap=False, seed=24,
                   features_col=DEFAULT_FEATURES_COL, anomaly_score_col=DEFAULT_ANOMALY_SCORE_COL,
                   prediction_col=DEFAULT_PREDICTION_COL):
        self._num_trees = num_trees
        self._max_samples = max_samples
        self._max_features = max_features
        self._max_depth = max_depth
        self._bootstrap = bootstrap
        self._seed = seed
        self._features_col = features_col
        self._anomaly_score_col = anomaly_score_col
        self._prediction_col = prediction_col

    def fit(self, data, columns, num_rows=None):
        self._columns = data.columns
        self._target_columns = columns
        self._num_rows = data.count() if num_rows is None else num_rows
        self._if = IForest(columns=columns, numRows=self._num_rows, numTrees=self._num_trees,
                           maxSamples=self._max_samples, maxFeatures=self._max_features,
                           maxDepth=self._max_depth, bootstrap=self._bootstrap, seed=self._seed,
                           featuresCol=self._features_col, anomalyScoreCol=self._anomaly_score_col)

        self._model = self._if.fit(data)

        summary = self._model.summary
        if not summary.success:
            raise Exception(summary.message)

        self._df = summary.dataWithScores
        return self

    def predict(self, contamination=0.05, relative_error=0.001):
        threshold = self._df.approxQuantile(self._anomaly_score_col, [1 - contamination], relative_error)[0]
        predicate = "{} > {}".format(self._anomaly_score_col, threshold)
        self._df = self._df.drop(self._prediction_col)
        self._df = self._df.withColumn(self._prediction_col, when(expr(predicate), 1).otherwise(0))
        return self._df

    def detect(self, contamination=0.05, relative_error=0.001):
        self.predict(contamination, relative_error)
        count = self._df.agg(_sum(self._prediction_col)).collect()[0][0]
        return count

    def remove(self, contamination=0.05, relative_error=0.001):
        self.predict(contamination, relative_error)
        predicate = "{} == 0".format(self._prediction_col)
        self._df = self._df.filter(predicate).drop(self._anomaly_score_col, self._prediction_col)
        return self._df

    def replace(self, values, contamination=0.05, relative_error=0.001):
        self.predict(contamination, relative_error)
        target_columns = []
        predicate = "{} == 1".format(self._prediction_col)
        idx = 0
        for c in self._columns:
            if c in self._target_columns:
                target_columns.append(when(expr(predicate), values[idx]).otherwise(col(c)).alias(c))
            else:
                target_columns.append(col(c))
        self._df = self._df.select(target_columns)
        return self._df
