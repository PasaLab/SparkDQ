from pyspark.sql.functions import when, expr, sum as _sum, col

from sparkdq.models.CommonUtils import DEFAULT_ANOMALY_SCORE_COL, DEFAULT_FEATURES_COL, DEFAULT_INDEX_COL, \
    DEFAULT_PREDICTION_COL
from sparkdq.models.LOF import LOF


class LOFOutlierSolver:
    """
    Local outlier factor outlier solver
    """
    def __init__(self, min_pts=5, dist_type="euclidean", features_col=DEFAULT_FEATURES_COL,
                 lof_col=DEFAULT_ANOMALY_SCORE_COL, prediction_col=DEFAULT_PREDICTION_COL):
        self._min_pts = min_pts
        self._dist_type = dist_type
        self._features_col = features_col
        self._lof_col = lof_col
        self._prediction_col = prediction_col
        self._lof = LOF(featuresCol=features_col, lofCol=lof_col, minPts=min_pts, distType=dist_type)

        self._df = None
        self._columns = None
        self._target_columns = None
        self._model = None

    def set_params(self, min_pts=5, dist_type="euclidean", features_col=DEFAULT_FEATURES_COL,
                   lof_col=DEFAULT_ANOMALY_SCORE_COL, prediction_col=DEFAULT_PREDICTION_COL):
        self._min_pts = min_pts
        self._dist_type = dist_type
        self._features_col = features_col
        self._lof_col = lof_col
        self._prediction_col = prediction_col
        self._lof = LOF(featuresCol=features_col, lofCol=lof_col, minPts=min_pts, distType=dist_type)

    def fit(self, data, columns, index_col=DEFAULT_INDEX_COL):
        self._columns = data.columns
        self._target_columns = columns
        self._lof = self._lof.setColumns(columns).setIndexCol(index_col)

        self._model = self._lof.fit(data)
        summary = self._model.summary
        if not summary.success:
            raise Exception(summary.message)

        self._df = summary.dataWithScores
        return self

    def predict(self, contamination=0.05, relative_error=0.001):
        threshold = self._df.approxQuantile(self._lof_col, [1 - contamination], relative_error)[0]
        predicate = "{} > {}".format(self._lof_col, threshold)
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
        self._df = self._df.filter(predicate).drop(self._lof_col, self._prediction_col)
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
