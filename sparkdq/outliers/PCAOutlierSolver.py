import math

import numpy as np
from pyspark.ml.feature import PCA, StandardScaler, VectorAssembler
from pyspark.ml.stat import Correlation
from pyspark.sql.functions import udf, mean, stddev, col, sum as _sum, when, expr
from pyspark.sql.types import DoubleType

from sparkdq.models.CommonUtils import DEFAULT_FEATURES_COL, DEFAULT_STANDARDIZED_FEATURES_COL, DEFAULT_DISTANCE_COL, \
    DEFAULT_PREDICTION_COL

DEFAULT_PCA_FEATURES_COL = "pca_features"


class PCAOutlierSolver:
    """
    Outlier solver by PCA reduction and Mahalanobois Distance
    """
    def __init__(self, k=3, features_col=DEFAULT_FEATURES_COL,
                 standardized_features_col=DEFAULT_STANDARDIZED_FEATURES_COL, distance_col=DEFAULT_DISTANCE_COL,
                 prediction_col=DEFAULT_PREDICTION_COL, pca_features_col=DEFAULT_PCA_FEATURES_COL):
        self._k = k
        self._features_col = features_col
        self._standardized_features_col = standardized_features_col
        self._pca_features_col = pca_features_col
        self._distance_col = distance_col
        self._prediction_col = prediction_col
        self._pca = PCA(k=self._k, inputCol=standardized_features_col, outputCol=pca_features_col)

        self._df = None
        self._columns = None
        self._target_columns = None
        self._model = None

    def set_params(self, k=3, features_col=DEFAULT_FEATURES_COL,
                   standardized_features_col=DEFAULT_STANDARDIZED_FEATURES_COL, distance_col=DEFAULT_DISTANCE_COL,
                   prediction_col=DEFAULT_PREDICTION_COL, pca_features_col=DEFAULT_PCA_FEATURES_COL):
        self._k = k
        self._features_col = features_col
        self._standardized_features_col = standardized_features_col
        self._pca_features_col = pca_features_col
        self._distance_col = distance_col
        self._prediction_col = prediction_col
        self._pca = PCA(k=k, inputCol=standardized_features_col, outputCol=pca_features_col)

    def fit(self, data, columns):
        self._columns = data.columns
        self._target_columns = columns
        assembler = VectorAssembler(inputCols=columns, outputCol=self._features_col)
        df_with_features = assembler.transform(data)
        scalar = StandardScaler(withMean=True, withStd=True, inputCol=self._features_col,
                                outputCol=self._standardized_features_col)
        df_with_standardized_features = scalar.fit(df_with_features).transform(df_with_features)
        self._model = self._pca.fit(df_with_standardized_features)
        df_with_pca_features = self._model.transform(df_with_standardized_features)
        self._df = PCAOutlierSolver._with_mahalanobis(df_with_pca_features, self._pca_features_col,
                                                      self._distance_col)
        # 此处df包含features、standardized features、pca features、distance
        return self

    def predict(self, method="quantile", contamination=0.05, relative_error=0.001, deviation=1.5):
        if method.lower() == "stddev":
            mean_stddev = self._df.agg(mean(self._distance_col), stddev(self._distance_col)).collect()[0]
            distance_threshold = mean_stddev[0] + deviation * mean_stddev[1]
        else:
            distance_threshold = self._df.approxQuantile(self._distance_col, [1 - contamination], relative_error)[0]

        self._df = self._df.drop(self._prediction_col)
        self._df = self._df\
            .withColumn(self._prediction_col,
                        when(expr("{} > {}".format(self._distance_col, distance_threshold)), 1).otherwise(0))
        # 此时df包含features、standardized features、pca features、distance、prediction
        return self._df

    def detect(self, method="quantile", contamination=0.05, relative_error=0.001, deviation=1.5):
        self.predict(method=method, contamination=contamination, relative_error=relative_error, deviation=deviation)
        outlier_count = self._df.agg(_sum(col(self._prediction_col)))
        return outlier_count

    def remove(self, method="quantile", contamination=0.05, relative_error=0.001, deviation=1.5):
        self.predict(method=method, contamination=contamination, relative_error=relative_error, deviation=deviation)
        self._df = self._df.filter("{} == 0".format(self._prediction_col)) \
            .drop(self._features_col, self._distance_col, self._prediction_col, self._standardized_features_col,
                  self._pca_features_col)
        return self._df

    def replace(self, values, method="quantile", contamination=0.05, relative_error=0.001, deviation=1.5):
        self.predict(method=method, contamination=contamination, relative_error=relative_error, deviation=deviation)
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

    @staticmethod
    def _with_mahalanobis(data, target_column, distance_col):
        coeff = Correlation.corr(data, target_column).collect()[0][0]
        inv_convariance = np.linalg.inv(coeff.toArray())

        def _compute_distance(col):
            left = np.array(col)
            right = left.T
            dist = left.dot(inv_convariance).dot(right)
            return math.sqrt(dist)

        dis_udf = udf(_compute_distance, DoubleType())
        return data.withColumn(distance_col, dis_udf(target_column))


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
    # pca = PCAOutlierSolver(k=3)
    # pca.fit(df, columns=["height", "weight"])
    # c, a = pca.detect(method="quantile", threshold=0.9)
    # print(c, a)
