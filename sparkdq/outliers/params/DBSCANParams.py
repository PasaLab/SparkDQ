import json

from sparkdq.models.CommonUtils import DEFAULT_CLUSTER_COL
from sparkdq.outliers.params.OutlierSolverParams import OutlierSolverParams
from sparkdq.outliers.OutlierSolver import OutlierSolver


class DBSCANParams(OutlierSolverParams):

    def __init__(self, cluster_col=DEFAULT_CLUSTER_COL, eps=0.5, min_pts=5, dist_type="euclidean", max_partitions=5):
        self.eps = eps
        self.min_pts = min_pts
        self.dist_type = dist_type
        self.max_partitions = max_partitions
        self.cluster_col = cluster_col

    def model(self):
        return OutlierSolver.DBSCAN

    @staticmethod
    def from_json(json_str):
        d = json.loads(json_str)
        return DBSCANParams(
            cluster_col=d["cluster_col"],
            eps=d["eps"],
            min_pts=d["min_pts"],
            dist_type=d["dist_type"],
            max_partitions=d["max_partitions"]
        )
