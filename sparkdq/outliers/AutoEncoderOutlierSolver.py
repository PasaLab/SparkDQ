from bigdl.nn.layer import *
from bigdl.nn.criterion import *
from bigdl.optim.optimizer import *
from bigdl.dataset.transformer import *
from pyspark.ml.feature import StandardScaler, VectorAssembler
from pyspark.ml.linalg import Vectors, VectorUDT
from pyspark.sql.functions import col, udf, mean as _mean, stddev, sum as _sum, when, expr
from pyspark.sql.types import DoubleType, StructType, StructField

from sparkdq.models.CommonUtils import DEFAULT_FEATURES_COL, DEFAULT_STANDARDIZED_FEATURES_COL, DEFAULT_PREDICTION_COL,\
    DEFAULT_INDEX_COL
from sparkdq.conf.Context import Context

DEFAULT_RECONSTRUCTED_FEATURES_COL = "reconstructed_features"
DEFAULT_RECONSTRUCTION_ERROR_COL = "reconstruction_error"


class AutoEncoderOutlierSolver:
    """
    Auto-encoder outlier solver
    """
    def __init__(self, input_size, hidden_size, batch_size=24, num_epochs=100, features_col=DEFAULT_FEATURES_COL,
                 standardized_features_col=DEFAULT_STANDARDIZED_FEATURES_COL, prediction_col=DEFAULT_PREDICTION_COL,
                 reconstructed_features_col=DEFAULT_RECONSTRUCTED_FEATURES_COL,
                 reconstructed_error_col=DEFAULT_RECONSTRUCTION_ERROR_COL):
        self._input_size = input_size
        self._hidden_size = hidden_size
        self._batch_size = batch_size
        self._num_epochs = num_epochs
        init_engine()
        self._features_col = features_col
        self._standardized_features_col = standardized_features_col
        self._prediction_col = prediction_col
        self._reconstructed_features_col = reconstructed_features_col
        self._reconstructed_error_col = reconstructed_error_col
        self._ae = AutoEncoderOutlierSolver._get_auto_encoder(input_size, hidden_size)

        self._df = None
        self._rdd = None
        self._model = None
        self._columns = None
        self._target_columns = None

    def set_params(self, input_size, hidden_size, batch_size=24, num_epochs=100, features_col=DEFAULT_FEATURES_COL,
                   standardized_features_col=DEFAULT_STANDARDIZED_FEATURES_COL, prediction_col=DEFAULT_PREDICTION_COL,
                   reconstructed_features_col=DEFAULT_RECONSTRUCTED_FEATURES_COL,
                   reconstructed_error_col=DEFAULT_RECONSTRUCTION_ERROR_COL):
        self._input_size = input_size
        self._hidden_size = hidden_size
        self._batch_size = batch_size
        self._num_epochs = num_epochs
        self._features_col = features_col
        self._standardized_features_col = standardized_features_col
        self._prediction_col = prediction_col
        self._reconstructed_features_col = reconstructed_features_col
        self._reconstructed_error_col = reconstructed_error_col
        self._ae = AutoEncoderOutlierSolver._get_auto_encoder(input_size, hidden_size)

    def fit(self, data, columns, index_col=DEFAULT_INDEX_COL):
        self._columns = data.columns
        self._target_columns = columns
        assembler = VectorAssembler(inputCols=columns, outputCol=self._features_col)
        df_with_features = assembler.transform(data)
        scalar = StandardScaler(withMean=True, withStd=True, inputCol=self._features_col,
                                outputCol=self._standardized_features_col)
        self._df = scalar.fit(df_with_features).transform(df_with_features)

        ids = [i[0] for i in self._df.select(index_col).collect()]
        self._rdd = self._df.select(index_col, self._standardized_features_col).rdd
        train_data = self._rdd.map(lambda r: Sample.from_ndarray(r[1].toArray(), r[1].toArray()))
        optimizer = Optimizer(
            model=self._ae,
            training_rdd=train_data,
            criterion=MSECriterion(),
            optim_method=Adam(),
            end_trigger=MaxEpoch(self._num_epochs),
            batch_size=self._batch_size
        )
        self._model = optimizer.optimize()
        reconstructed_data = [Vectors.dense(e) for e in self._model.predict(train_data).collect()]
        tmp_schema = StructType([
            self._df.schema[index_col],
            StructField(self._reconstructed_features_col, VectorUDT(), True),
        ])

        tmp_data = zip(ids, reconstructed_data)
        tmp_df = Context().spark.createDataFrame(tmp_data, tmp_schema)
        self._df = self._df.join(tmp_df, on=index_col, how="inner")

        compute_reconstruction_error = udf(lambda x, y: float(x.squared_distance(y)), DoubleType())
        self._df = self._df.withColumn(
            self._reconstructed_error_col,
            compute_reconstruction_error(col(self._standardized_features_col), col(self._reconstructed_features_col)))\
            .sort(index_col)
        return self

    def predict(self, method="quantile", contamination=0.05, relative_error=0.001, deviation=1.5):
        if method.lower() == "quantile":
            distance_threshold = self._df.approxQuantile(self._reconstructed_error_col, [1-contamination],
                                                         relative_error)[0]
        else:
            mean_stddev = self._df\
                .agg(_mean(self._reconstructed_error_col), stddev(self._reconstructed_error_col))\
                .collect()[0]
            distance_threshold = mean_stddev[0] + deviation * mean_stddev[1]
        self._df = self._df.drop(self._prediction_col)
        self._df = self._df \
            .withColumn(self._prediction_col,
                        when(expr("{} > {}".format(self._reconstructed_error_col, distance_threshold)), 1).otherwise(0))
        return self._df

    def detect(self, method="quantile", contamination=0.05, relative_error=0.001, deviation=1.5):
        self.predict(method=method, contamination=contamination, relative_error=relative_error, deviation=deviation)
        outlier_count = self._df.agg(_sum(col(self._prediction_col))).collect()[0][0]
        return outlier_count

    def remove(self, method="quantile", contamination=0.05, relative_error=0.001, deviation=1.5):
        self.predict(method=method, contamination=contamination, relative_error=relative_error, deviation=deviation)
        self._df = self._df.filter("{} == 0".format(self._prediction_col))\
            .drop(self._features_col, self._standardized_features_col, self._reconstructed_features_col,
                  self._reconstructed_error_col, self._prediction_col)
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
    def _get_auto_encoder(input_size, hidden_size):
        # init
        module = Sequential()

        # encoder layers
        module.add(Linear(input_size, hidden_size))
        module.add(ReLU())

        # decoder layers
        module.add(Linear(hidden_size, input_size))
        module.add(Sigmoid())
        return module


if __name__ == "__main__":
    pass

    # ctx = Context()
    # spark = ctx.spark
    # sc = ctx.sc
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
    #     (15, 'F', 20, 190, 75)
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
    # ae = AutoEncoderOutlierSolver(2, 1, batch_size=12)
    # ae.fit(df, columns=["height", "weight"], index_column="id")
    # result = ae.predict(method="stddev", threshold=1.5)
    # result.show()
