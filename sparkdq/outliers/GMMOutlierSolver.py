import math

import numpy as np
from pyspark.ml.clustering import GaussianMixture
from pyspark.ml.feature import VectorAssembler
from pyspark.sql.functions import udf, avg, col, stddev_pop, when, sum as _sum, expr
from pyspark.sql.types import DoubleType

from sparkdq.models.CommonUtils import DEFAULT_FEATURES_COL, DEFAULT_CLUSTER_COL, DEFAULT_DISTANCE_COL, \
    DEFAULT_PREDICTION_COL

DEFAULT_PROBABILITY_COL = "probability"


class GMMOutlierSolver:
    """
    Gaussian Mixture model outlier solver
    """
    def __init__(self, k=2, max_iter=100, tol=0.01, seed=None, features_col=DEFAULT_FEATURES_COL,
                 probability_col=DEFAULT_PROBABILITY_COL, cluster_col=DEFAULT_CLUSTER_COL,
                 distance_col=DEFAULT_DISTANCE_COL, prediction_col=DEFAULT_PREDICTION_COL):
        self._k = k
        self._max_iter = max_iter
        self._seed = seed
        self._tol = tol
        self._features_col = features_col
        self._probability_col = probability_col
        self._cluster_col = cluster_col
        self._distance_col = distance_col
        self._prediction_col = prediction_col
        self._gm = GaussianMixture(k=k, maxIter=max_iter, tol=tol, seed=seed, featuresCol=features_col,
                                   probabilityCol=probability_col, predictionCol=cluster_col)

        self._num_rows = None
        self._df = None
        self._columns = None
        self._target_columns = None
        self._model = None

    def set_params(self, k=2, max_iter=100, tol=0.01, seed=None, features_col=DEFAULT_FEATURES_COL,
                   probability_col=DEFAULT_PROBABILITY_COL, cluster_col=DEFAULT_CLUSTER_COL,
                   distance_col=DEFAULT_DISTANCE_COL, prediction_col=DEFAULT_PREDICTION_COL):
        self._k = k
        self._max_iter = max_iter
        self._tol = tol
        self._seed = seed
        self._features_col = features_col
        self._probability_col = probability_col
        self._cluster_col = cluster_col
        self._distance_col = distance_col
        self._prediction_col = prediction_col

        self._gm = GaussianMixture(k=k, maxIter=max_iter, tol=tol, seed=seed, predictionCol=cluster_col,
                                   probabilityCol=probability_col, featuresCol=features_col)
        return self

    def fit(self, data, columns, num_rows=None):
        # save original columns and target columns
        self._columns = data.columns
        self._target_columns = columns
        self._num_rows = data.count() if num_rows is None else num_rows

        assembler = VectorAssembler(inputCols=columns, outputCol=self._features_col)
        df_with_features = assembler.transform(data)
        self._model = self._gm.fit(df_with_features)
        df_with_cluster = self._model.transform(df_with_features)
        mean = self._model.gaussiansDF.select("mean").collect()
        cov = self._model.gaussiansDF.select("cov").collect()
        compute_distance = self._udf_compute_distance(mean, cov)
        self._df = df_with_cluster.withColumn(
            self._distance_col,
            compute_distance(self._features_col, self._cluster_col)
        )
        # 此时df包含四个新列：features、cluster、probability、distance
        return self

    @staticmethod
    def _udf_compute_distance(mean, cov):
        mean = [m[0].toArray() for m in mean]
        cov = [c[0].toArray() for c in cov]

        def _compute_distance(features, prediction):
            mu = mean[prediction]
            matrix_s = np.linalg.inv(cov[prediction])
            diff = features - mu
            left = np.array([diff])
            right = left.T
            dist = left.dot(matrix_s).dot(right)
            return math.sqrt(dist)

        return udf(_compute_distance, DoubleType())

    def predict(self, min_cluster=1, deviation=1.5):
        if (min_cluster >= 0) and (min_cluster < 1):
            min_cluster = self._num_rows * min_cluster

        mean_covs = self._df.select([self._cluster_col, self._distance_col])\
            .groupBy(self._cluster_col)\
            .agg(avg(col(self._distance_col)), stddev_pop(col(self._distance_col)))\
            .sort(self._cluster_col)\
            .collect()
        size = self._model.summary.clusterSizes

        # 较小的簇和偏离簇中心的点视为异常
        thresholds = dict()
        small_clusters = []
        for i in range(len(size)):
            info = mean_covs[i]
            if size[i] < min_cluster:
                small_clusters.append(i)
            else:
                thresholds[i] = info["avg({})".format(self._distance_col)] + \
                                                          deviation * info["stddev_pop({})".format(self._distance_col)]

        self._df = self._df.drop(self._prediction_col)

        judge_outlier_func = udf(
            lambda index, distance:
            1 if (index in small_clusters) or ((index in thresholds) and (distance > thresholds[index])) else 0
        )
        self._df = self._df.withColumn(self._prediction_col,
                                       judge_outlier_func(col(self._cluster_col), col(self._distance_col)))
        # 此时df包含features、cluster、distance、probability、prediction
        return self._df

    def detect(self, min_cluster=1, deviation=1.5):
        self.predict(min_cluster=min_cluster, deviation=deviation)
        outlier_count = self._df.agg(_sum(self._prediction_col)).collect()[0][0]
        # 保留所有的信息列，以便使用其他参数进行统计
        return int(outlier_count)

    def remove(self, min_cluster=1, deviation=1.5):
        self.predict(min_cluster=min_cluster, deviation=deviation)
        self._df = self._df.filter("{} == 0".format(self._prediction_col))\
            .drop(self._features_col, self._cluster_col, self._probability_col, self._distance_col,
                  self._prediction_col)
        # 删除所有额外信息列
        return self._df

    def replace(self, values, min_cluster=1, deviation=1.5):
        self.predict(min_cluster=min_cluster, deviation=deviation)
        target_columns = []
        predicate = "{} == 1".format(self._prediction_col)
        idx = 0
        for c in self._columns:
            if c in self._target_columns:
                target_columns.append(when(expr(predicate), values[idx]).otherwise(col(c)).alias(c))
            else:
                target_columns.append(col(c))
        self._df = self._df.select(target_columns)
        # 只选择初始列，相当于删除所有额外信息列
        return self._df

    def get_data(self):
        return self._df


if __name__ == '__main__':
    pass
    # from pyspark.sql import SparkSession
    #
    # spark = SparkSession.builder.master('local').getOrCreate()
    # sc = spark.sparkContext
    #
    # rdd = spark.sparkContext.parallelize([
    #     (1, "A", 19, 181, 67),
    #     (2, "C", 17, 179, 67),
    #     (3, 'E', 18, 180, 68),
    #     (4, 'E', 29, 180, 68),
    #     (5, 'E', 18, 180, 68),
    #     (6, 'E', 18, 180, 68),
    #     (7, 'E', 18, 180, 68),
    #     (8, 'E', 18, -180, 68),
    #     (9, 'F', 28, 21, 7),
    #     (10, 'F', 28, 22, 8),
    #     (11, 'F', 28, 22, 8),
    #     (12, 'F', 28, 22, 8),
    #     (13, 'F', 28, 22, 8),
    #     (14, 'F', 28, 23, 7),
    # ])
    # from pyspark.sql.types import StructType, StructField, LongType, StringType, IntegerType
    #
    # schema = StructType([
    #     StructField("id", LongType(), True),
    #     StructField("name", StringType(), True),
    #     StructField("age", LongType(), True),
    #     StructField("height", IntegerType(), True),
    #     StructField("weight", IntegerType(), True)
    # ])
    # df = spark.createDataFrame(rdd, schema)
    #
    # kd = GMMOutlierSolver(k=2, max_iter=100, tol=0.01)
    # kd.fit(df, ['height', 'weight'])
    # count = kd.detect(2, 1.5)
    # print(count)
