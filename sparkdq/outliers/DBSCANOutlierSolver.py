from pyspark.sql.functions import col, when, sum as _sum, expr

from sparkdq.models.CommonUtils import DEFAULT_CLUSTER_COL, DEFAULT_INDEX_COL
from sparkdq.models.dbscan.DBSCAN import DBSCAN


class DBSCANOutlierSolver:
    """
    Density-based spatial clustering outlier solver
    """
    def __init__(self, eps=0.5, min_pts=5, dist_type="euclidean", max_partitions=5, cluster_col=DEFAULT_CLUSTER_COL):
        self._eps = eps
        self._min_pts = min_pts
        self._dist_type = dist_type
        self._max_partitions = max_partitions
        self._cluster_col = cluster_col

        self._df = None
        self._columns = None
        self._target_columns = None
        self._model = DBSCAN(eps=eps, min_pts=min_pts, dist_type=dist_type, max_partitions=max_partitions,
                             prediction_col=cluster_col)

    def set_params(self, eps=0.5, min_pts=5, dist_type="euclidean", max_partitions=5, cluster_col=DEFAULT_CLUSTER_COL):
        self._eps = eps
        self._min_pts = min_pts
        self._dist_type = dist_type
        self._max_partitions = max_partitions
        self._cluster_col = cluster_col
        self._model = DBSCAN(eps=eps, min_pts=min_pts, dist_type=dist_type, max_partitions=max_partitions,
                             prediction_col=cluster_col)

    def fit(self, data, columns, index_col=DEFAULT_INDEX_COL):
        self._df = data
        self._columns = data.columns
        self._target_columns = columns
        self._df = self._model.transform(data, columns, index_col)
        return self

    def detect(self):
        outlier_count = self._df.agg(_sum(col(self._cluster_col)))
        return outlier_count

    def remove(self):
        self._df = self._df.filter("{} != -1".format(self._cluster_col)).drop(self._cluster_col)
        return self._df

    def replace(self, values):
        target_columns = []
        predicate = "{} == -1".format(self._cluster_col)
        idx = 0
        for c in self._columns:
            if c in self._target_columns:
                target_columns.append(when(expr(predicate), values[idx]).otherwise(col(c)).alias(c))
            else:
                target_columns.append(col(c))
        self._df = self._df.select(target_columns)
        return self._df

    def get_data(self):
        return self._df
