import math

from pyspark.sql.functions import sum as _sum, expr, avg, abs as _abs, col
from pyspark.sql.window import Window

from sparkdq.analytics.Analyzer import ComplexAnalyzer
from sparkdq.analytics.analyzers.ApproxQuantile import ApproxQuantile
from sparkdq.analytics.analyzers.StandardDeviation import StandardDeviation
from sparkdq.analytics.catalyst.DQFunctions import stateful_stddev, stateful_approx_quantile
from sparkdq.analytics.CommonFunctions import metric_from_failure, metric_from_value, metric_from_empty, \
    conditional_selection, MAD_COL, SMA_COL
from sparkdq.analytics.metrics.DoubleMetric import DoubleMetric
from sparkdq.analytics.Preconditions import is_numeric, has_column
from sparkdq.analytics.states.ApproxQuantileState import ApproxQuantileState
from sparkdq.analytics.states.NumMatchesAndRows import NumMatchesAndRows
from sparkdq.exceptions.AnalysisExceptions import AnalysisException
from sparkdq.exceptions.CommonExceptions import UnsupportedOutlierModelException, InconsistentParametersException
from sparkdq.models.CommonUtils import DEFAULT_INDEX_COL
from sparkdq.outliers.AutoEncoderOutlierSolver import AutoEncoderOutlierSolver
from sparkdq.outliers.DBSCANOutlierSolver import DBSCANOutlierSolver
from sparkdq.outliers.GMMOutlierSolver import GMMOutlierSolver
from sparkdq.outliers.IForestOutlierSolver import IForestOutlierSolver
from sparkdq.outliers.KMeansOutlierSolver import KMeansOutlierSolver
from sparkdq.outliers.LOFOutlierSolver import LOFOutlierSolver
from sparkdq.outliers.OutlierSolver import OutlierSolver
from sparkdq.outliers.params.OutlierSolverParams import OutlierSolverParams
from sparkdq.outliers.PCAOutlierSolver import PCAOutlierSolver
from sparkdq.structures.Entity import Entity


class Outliers(ComplexAnalyzer):

    def __init__(self, column_or_columns, model, model_params, where=None, index_col=DEFAULT_INDEX_COL):
        """
        :param column_or_columns: str or list of str, columns to do the detection
        :param index_col: str, index column
        :param model: enum, the chosen model to detect outliers
        :param model_params: related Params class params for the chosen model
        :param where
        """
        if not isinstance(column_or_columns, list):
            column_or_columns = [column_or_columns]
        column_or_columns = sorted(column_or_columns)
        entity = Entity.Multicolumn if len(column_or_columns) > 1 else Entity.Column
        super(Outliers, self).__init__("{}({})".format(Outliers.__name__, model.value), entity, column_or_columns)
        self.model = model
        self.index_col = index_col
        self.model_params = model_params
        self.where = where

    def additional_preconditions(self):
        return [is_numeric(col) for col in self.instance] +\
               [Outliers.param_check(self.model, self.model_params, self.instance), has_column(self.index_col)]

    def has_additional_analyzers(self):
        models = [OutlierSolver.kSigma, OutlierSolver.SMA, OutlierSolver.TukeyTest, OutlierSolver.MAD]
        return self.model in models

    def additional_analyzers(self):
        if (self.model == OutlierSolver.kSigma) or (self.model == OutlierSolver.SMA):
            return [StandardDeviation(self.instance[0])]
        elif (self.model == OutlierSolver.TukeyTest) or (self.model == OutlierSolver.MAD):
            return [ApproxQuantile(self.instance[0], 0.5, self.model_params.relative_error)]
        return []

    def compute_state_from_data(self, data, num_rows=None, state_provider=None):
        if num_rows is None:
            num_rows = data.count()
        if self.model == OutlierSolver.kSigma:
            return self.count_by_k_sigma(data, state_provider)
        elif self.model == OutlierSolver.TukeyTest:
            return self.count_by_tukey_test(data, num_rows, state_provider)
        elif self.model == OutlierSolver.MAD:
            return self.count_by_mad(data, num_rows, state_provider)
        elif self.model == OutlierSolver.SMA:
            return self.count_by_sma(data, state_provider)
        elif self.model == OutlierSolver.GMM:
            return self.count_by_gmm(data, num_rows)
        elif self.model == OutlierSolver.KMeans:
            return self.count_by_kmeans(data, num_rows)
        elif self.model == OutlierSolver.DBSCAN:
            return self.count_by_dbscan(data, num_rows)
        elif self.model == OutlierSolver.LOF:
            return self.count_by_lof(data, num_rows)
        elif self.model == OutlierSolver.IForest:
            return self.count_by_iforest(data, num_rows)
        elif self.model == OutlierSolver.PCA:
            return self.count_by_pca(data, num_rows)
        elif self.model == OutlierSolver.AutoEncoder:
            return self.count_by_auto_encoder(data, num_rows)
        raise UnsupportedOutlierModelException("Unsupported outlier model {} for {}!".format(self.model, self))

    def compute_metric_from_state(self, state):
        cls = DoubleMetric
        if state is None:
            return metric_from_empty(self, cls)
        return metric_from_value(self, cls, state.metric_value())

    def to_failure_metric(self, exception):
        cls = DoubleMetric
        return metric_from_failure(self, cls, AnalysisException.wrap_if_necessary(exception))

    def to_json(self):
        return {
            "AnalyzerName": Outliers.__name__,
            "Columns": self.instance,
            "Model": self.model.value,
            "ModelParams": self.model_params.to_json(),
            "IndexCol": self.index_col,
            "Where": self.where
        }

    @staticmethod
    def from_json(d):
        return Outliers(d["Columns"], OutlierSolver(d["Model"]), OutlierSolverParams.from_json(d["ModelParams"]),
                        where=d["Where"], index_col=d["IndexCol"])

    @staticmethod
    def param_check(model, model_params, columns):
        def _param_check(_):
            single_col_models = [OutlierSolver.kSigma, OutlierSolver.TukeyTest, OutlierSolver.MAD, OutlierSolver.SMA]
            if (model in single_col_models) and (len(columns) > 1):
                raise InconsistentParametersException("Too many columns for single column model {}!"
                                                      .format(str(model)))
            params_model = model_params.model()
            if model != params_model:
                raise InconsistentParametersException("Inconsistent model between {} and {}.".format(model,
                                                                                                     model_params))
            # 各个参数类内部检查
        return _param_check

    def count_by_k_sigma(self, data, state_provider=None):
        column = self.instance[0]
        deviation = self.model_params.deviation
        if (state_provider is not None) and (state_provider.has_state(self.additional_analyzers()[0])):
            state = state_provider.load(self.additional_analyzers()[0])
            stddev = state.metric_value()
            n = state.n
            avg = state.avg
            lower_bound = avg - deviation * stddev
            upper_bound = avg + deviation * stddev
        else:
            pre_func = stateful_stddev(conditional_selection(column, self.where))
            pre_res = data.agg(pre_func).collect()[0][0]
            n, avg, m2 = pre_res[0], pre_res[1], pre_res[2]
            stddev = math.sqrt(m2 / n)
            lower_bound = avg - deviation * stddev
            upper_bound = avg + deviation * stddev

        predicate = "({} < {}) or ({} > {})".format(column, lower_bound, column, upper_bound)
        count_func = _sum(conditional_selection(expr(predicate), self.where).cast("integer"))
        count_res = data.agg(count_func).collect()[0][0]
        return NumMatchesAndRows(count_res, n)

    def count_by_tukey_test(self, data, num_rows, state_provider=None):
        column = self.instance[0]
        deviation = self.model_params.deviation
        relative_error = self.model_params.relative_error
        if num_rows is None:
            num_rows = data.count()
        if (state_provider is not None) and (state_provider.has_state(self.additional_analyzers()[0])):
            quantile_summaries = state_provider.load(self.additional_analyzers()[0])
        else:
            quantile_func = stateful_approx_quantile(conditional_selection(column, self.where), relative_error)
            res = data.agg(quantile_func).collect()[0][0]
            quantile_summaries = ApproxQuantileState.quantile_summaries_from_bytes(res)
        q13 = quantile_summaries.query_by_quantiles([0.25, 0.75])
        q1 = q13["0.25"]
        q3 = q13["0.75"]
        iqr = q3 - q1
        lower_bound = q1 - deviation * iqr
        upper_bound = q3 + deviation * iqr
        predicate = "({} < {}) or ({} > {})".format(column, lower_bound, column, upper_bound)
        count_func = _sum(conditional_selection(expr(predicate), self.where).cast("integer"))
        count_res = data.agg(count_func).collect()[0]
        return NumMatchesAndRows(count_res, num_rows)

    def count_by_mad(self, data, num_rows, state_provider=None):
        column = self.instance[0]
        deviation = self.model_params.deviation
        relative_error = self.model_params.relative_error
        if num_rows is None:
            num_rows = data.count()
        if (state_provider is not None) and (state_provider.has_state(self.additional_analyzers()[0])):
            quantile_summaries = state_provider.load(self.additional_analyzers()[0])
        else:
            quantile_func = stateful_approx_quantile(conditional_selection(column, self.where), relative_error)
            res = data.agg(quantile_func).collect()[0][0]
            quantile_summaries = ApproxQuantileState.quantile_summaries_from_bytes(res)
        median = quantile_summaries.query_by_quantile(0.5)

        mad_col = MAD_COL
        mad = data.withColumn(mad_col, abs(data[column]-median)).approxQuantile(mad_col, [0.5], 0.01)[0]
        lower_bound = median - deviation * mad
        upper_bound = median + deviation * mad
        predicate = "({} < {}) or ({} > {})".format(column, lower_bound, column, upper_bound)
        count_func = _sum(conditional_selection(expr(predicate), self.where).cast("integer"))
        count_res = data.agg(count_func).collect()[0]
        return NumMatchesAndRows(count_res, num_rows)

    def count_by_sma(self, data, state_provider=None):
        column = self.instance[0]
        deviation = self.model_params.deviation
        radius = self.model_params.radius
        if (state_provider is not None) and (state_provider.has_state(self.additional_analyzers()[0])):
            state = state_provider.load(self.additional_analyzers()[0])
            stddev = state.metric_value()
            n = state.n
        else:
            stddev_func = stateful_stddev(conditional_selection(column, None))
            stddev_res = data.agg(stddev_func).collect()[0][0]
            n, _, m2 = stddev_res[0], stddev_res[1], stddev_res[2]
            stddev = math.sqrt(m2 / n)
        bound = stddev * deviation

        window = Window.rangeBetween(-radius, radius)
        sma_col = SMA_COL
        count = data.withColumn(sma_col, avg(data[column]).over(window))\
            .agg(_sum((_abs(col(sma_col) - col(column)) > bound).cast("double")))\
            .collect()[0][0]
        return NumMatchesAndRows(count, n)

    def count_by_gmm(self, data, num_rows):
        k, max_iter, tol, seed = self.model_params.k, self.model_params.max_iter, self.model_params.tol, \
                                 self.model_params.seed
        min_cluster, deviation = self.model_params.min_cluster, self.model_params.deviation
        gmm = GMMOutlierSolver(k=k, max_iter=max_iter, tol=tol, seed=seed, features_col=self.model_params.features_col,
                               probability_col=self.model_params.probability_col,
                               cluster_col=self.model_params.cluster_col, distance_col=self.model_params.distance_col,
                               prediction_col=self.model_params.prediction_col)
        gmm.fit(data, self.instance)
        count = gmm.detect(min_cluster, deviation)
        return NumMatchesAndRows(count, num_rows)

    def count_by_kmeans(self, data, num_rows):
        k, max_iter, seed = self.model_params.k, self.model_params.max_iter, self.model_params.seed
        min_cluster, deviation = self.model_params.min_cluster, self.model_params.deviation
        kmeans = KMeansOutlierSolver(k=k, max_iter=max_iter, seed=seed, features_col=self.model_params.features_col,
                                     cluster_col=self.model_params.cluster_col,
                                     distance_col=self.model_params.distance_col,
                                     prediction_col=self.model_params.prediction_col,
                                     normalize=self.model_params.normalize,
                                     normalized_features_col=self.model_params.normalized_features_col)
        kmeans.fit(data, self.instance, num_rows=num_rows)
        count = kmeans.detect(min_cluster, deviation)
        return NumMatchesAndRows(count, num_rows)

    def count_by_dbscan(self, data, num_rows):
        eps, min_pts, dist_type, max_partitions = self.model_params.eps, self.model_params.min_pts, \
                                                  self.model_params.dist_type, self.model_params.max_partitions
        dbscan = DBSCANOutlierSolver(eps=eps, min_pts=min_pts, max_partitions=max_partitions, dist_type=dist_type,
                                     cluster_col=self.model_params.cluster_col)
        dbscan.fit(data, self.instance, index_col=self.index_col)
        count = dbscan.detect()
        return NumMatchesAndRows(count, num_rows)

    def count_by_lof(self, data, num_rows):
        min_pts, dist_type, contamination, relative_error = self.model_params.min_pts, self.model_params.dist_type, \
                                                            self.model_params.contamination, \
                                                            self.model_params.relative_error
        lof = LOFOutlierSolver(min_pts=min_pts, dist_type=dist_type, features_col=self.model_params.features_col,
                               lof_col=self.model_params.lof_col, prediction_col=self.model_params.prediction_col)
        lof.fit(data, self.instance, index_col=self.index_col)
        count = lof.detect(contamination, relative_error)
        return NumMatchesAndRows(count, num_rows)

    def count_by_iforest(self, data, num_rows):
        num_trees, max_samples, max_features, max_depth, bootstrap, seed = self.model_params.num_trees, \
                                                                           self.model_params.max_samples, \
                                                                           self.model_params.max_features, \
                                                                           self.model_params.max_depth, \
                                                                           self.model_params.bootstrap, \
                                                                           self.model_params.seed
        contamination, relative_error = self.model_params.contamination, self.model_params.relative_error
        iforest = IForestOutlierSolver(num_trees=num_trees, max_samples=max_samples, max_features=max_features,
                                       max_depth=max_depth, bootstrap=bootstrap, seed=seed,
                                       features_col=self.model_params.features_col,
                                       anomaly_score_col=self.model_params.anomaly_score_col,
                                       prediction_col=self.model_params.prediction_col)
        iforest.fit(data, self.instance, num_rows=num_rows)
        count = iforest.detect(contamination, relative_error)
        return NumMatchesAndRows(count, num_rows)

    def count_by_pca(self, data, num_rows):
        k = self.model_params.k
        method, contamination, relative_error, deviation = self.model_params.method, self.model_params.contamination, \
            self.model_params.relative_error, self.model_params.deviation
        pca = PCAOutlierSolver(k=k, features_col=self.model_params.features_col,
                               standardized_features_col=self.model_params.standardized_features_col,
                               distance_col=self.model_params.distance_col,
                               prediction_col=self.model_params.prediction_col,
                               pca_features_col=self.model_params.pca_features_col)
        pca.fit(data, self.instance)
        count = pca.detect(method=method, contamination=contamination, relative_error=relative_error,
                           deviation=deviation)
        return NumMatchesAndRows(count, num_rows)

    def count_by_auto_encoder(self, data, num_rows):
        input_size, hidden_size, batch_size, num_epochs = self.model_params.input_size, self.model_params.hidden_size, \
            self.model_params.batch_size, self.model_params.num_epochs
        method, contamination, relative_error, deviation = self.model_params.method, self.model_params.contamination, \
            self.model_params.relative_error, self.model_params.deviation
        auto_encoder = AutoEncoderOutlierSolver(input_size=input_size, hidden_size=hidden_size, batch_size=batch_size,
                                                num_epochs=num_epochs, features_col=self.model_params.features_col,
                                                standardized_features_col=self.model_params.standardized_features_col,
                                                prediction_col=self.model_params.prediction_col,
                                                reconstructed_features_col=self.model_params.reconstructed_features_col,
                                                reconstructed_error_col=self.model_params.reconstructed_error_col)
        auto_encoder.fit(data, self.instance, self.index_col)
        count = auto_encoder.detect(method=method, contamination=contamination, relative_error=relative_error,
                                    deviation=deviation)
        return NumMatchesAndRows(count, num_rows)
