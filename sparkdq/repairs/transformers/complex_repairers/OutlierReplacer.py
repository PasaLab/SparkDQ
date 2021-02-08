import math

from pyspark.sql.functions import expr, avg, abs as _abs, col, when
from pyspark.sql.window import Window

from sparkdq.analytics.analyzers.ApproxQuantile import ApproxQuantile
from sparkdq.analytics.analyzers.StandardDeviation import StandardDeviation
from sparkdq.analytics.catalyst.DQFunctions import stateful_stddev, stateful_approx_quantile
from sparkdq.analytics.CommonFunctions import conditional_selection, SMA_COL, MAD_COL
from sparkdq.analytics.Preconditions import has_columns, is_numeric
from sparkdq.analytics.states.ApproxQuantileState import ApproxQuantileState
from sparkdq.exceptions.CommonExceptions import UnsupportedOutlierModelException, InconsistentParametersException
from sparkdq.models.CommonUtils import DEFAULT_INDEX_COL
from sparkdq.outliers.AutoEncoderOutlierSolver import AutoEncoderOutlierSolver
from sparkdq.outliers.DBSCANOutlierSolver import DBSCANOutlierSolver
from sparkdq.outliers.GMMOutlierSolver import GMMOutlierSolver
from sparkdq.outliers.IForestOutlierSolver import IForestOutlierSolver
from sparkdq.outliers.KMeansOutlierSolver import KMeansOutlierSolver
from sparkdq.outliers.LOFOutlierSolver import LOFOutlierSolver
from sparkdq.outliers.OutlierSolver import OutlierSolver
from sparkdq.outliers.PCAOutlierSolver import PCAOutlierSolver
from sparkdq.repairs.transformers.complex_repairers.ComplexRepairer import ComplexRepairer


class OutlierReplacer(ComplexRepairer):

    def __init__(self, column_or_columns, model, model_params, value_or_values, index_col=DEFAULT_INDEX_COL, where=None):
        self.columns = column_or_columns if isinstance(column_or_columns, list) else list(column_or_columns)
        self.index_col = index_col
        self.model = model
        self.model_params = model_params
        self.where = where
        self.values = [str(x) for x in value_or_values] if isinstance(value_or_values, list) \
            else list(str(value_or_values))

    def transform(self, data, state_provider=None, check=True):
        if check:
            for condition in self.preconditions():
                condition(data.schema)
        if self.model == OutlierSolver.kSigma:
            self.replace_by_k_sigma(data)
        elif self.model == OutlierSolver.TukeyTest:
            return self.replace_by_tukey_test(data, state_provider)
        elif self.model == OutlierSolver.MAD:
            return self.replace_by_mad(data, state_provider)
        elif self.model == OutlierSolver.SMA:
            return self.replace_by_sma(data, state_provider)
        elif self.model == OutlierSolver.GMM:
            return self.replace_by_gmm(data)
        elif self.model == OutlierSolver.KMeans:
            return self.replace_by_kmeans(data)
        elif self.model == OutlierSolver.DBSCAN:
            return self.replace_by_dbscan(data)
        elif self.model == OutlierSolver.LOF:
            return self.replace_by_lof(data)
        elif self.model == OutlierSolver.IForest:
            return self.replace_by_iforest(data)
        elif self.model == OutlierSolver.PCA:
            return self.replace_by_pca(data)
        elif self.model == OutlierSolver.AutoEncoder:
            return self.replace_by_auto_encoder(data)
        else:
            raise UnsupportedOutlierModelException("Unsupported outlier model {}!".format(self.model))

    def has_additional_analyzers(self):
        return self.model in [OutlierSolver.kSigma, OutlierSolver.TukeyTest, OutlierSolver.MAD, OutlierSolver.SMA]

    def additional_analyzers(self):
        if (self.model == OutlierSolver.kSigma) or (self.model == OutlierSolver.SMA):
            return [StandardDeviation(self.columns[0], None)]
        elif (self.model == OutlierSolver.TukeyTest) or (self.model == OutlierSolver.MAD):
            return [ApproxQuantile(self.columns[0], 0.5, self.model_params.relative_error)]
        return []

    def preconditions(self):
        return [OutlierReplacer.param_check(self.model, self.model_params, self.columns, self.values),
                has_columns(self.columns)] +\
               [is_numeric(col) for col in self.columns]

    @staticmethod
    def param_check(model, model_params, columns, values):
        def _param_check(_):
            single_col_models = [OutlierSolver.kSigma, OutlierSolver.TukeyTest, OutlierSolver.MAD, OutlierSolver.SMA]
            if (model in single_col_models) and (len(columns) > 1):
                raise InconsistentParametersException("{} can only be used for single column detection!"
                                                      .format(str(model)))
            model_from_params = model_params.model()
            if model != model_from_params:
                raise InconsistentParametersException("Inconsistent model between {} and {}."
                                                      .format(model, model_params))
            if len(columns) != len(values):
                raise InconsistentParametersException("Replacement values and columns should be equal in quantity, but "
                                                      "there are {} columns and {} replacements!".format(len(columns),
                                                                                                         len(values)))
        return _param_check

    def replace_by_k_sigma(self, data, state_provider=None):
        column = self.columns[0]
        value = self.values[0]
        deviation = self.model_params.deviation
        if (state_provider is not None) and (state_provider.has_state(self.additional_analyzers()[0])):
            state = state_provider.load(self.additional_analyzers()[0])
            stddev = state.metric_value()
            avg = state.avg
            lower_bound = avg - deviation * stddev
            upper_bound = avg + deviation * stddev
        else:
            pre_func = stateful_stddev(conditional_selection(column, None))
            pre_res = data.agg(pre_func).collect()[0][0]
            n, avg, m2 = pre_res[0], pre_res[1], pre_res[2]
            stddev = math.sqrt(m2 / n)
            lower_bound = avg - deviation * stddev
            upper_bound = avg + deviation * stddev
        predicate = "({} < {}) or ({} > {})".format(column, lower_bound, column, upper_bound)
        return data.withColumn(column, when(expr(predicate), value).otherwise(col(column)))

    def replace_by_tukey_test(self, data, state_provider=None):
        column = self.columns[0]
        value = self.values[0]
        deviation = self.model_params.deviation
        relative_error = self.model_params.relative_error
        if (state_provider is not None) and (state_provider.has_state(self.additional_analyzers()[0])):
            quantile_summaries = state_provider.load(self.additional_analyzers()[0])
        else:
            quantile_func = stateful_approx_quantile(column, relative_error)
            res = data.agg(quantile_func).collect()[0][0]
            quantile_summaries = ApproxQuantileState.quantile_summaries_from_bytes(res)
        q13 = quantile_summaries.query_by_quantiles([0.25, 0.75])
        q1 = q13["0.25"]
        q3 = q13["0.75"]
        iqr = q3 - q1
        lower_bound = q1 - deviation * iqr
        upper_bound = q3 + deviation * iqr
        predicate = "({} < {}) or ({} > {})".format(column, lower_bound, column, upper_bound)
        return data.withColumn(column, when(expr(predicate), value).otherwise(col(column)))

    def replace_by_mad(self, data, state_provider=None):
        column = self.columns[0]
        value = self.values[0]
        deviation = self.model_params.deviation
        relative_error = self.model_params.relative_error
        if (state_provider is not None) and (state_provider.has_state(self.additional_analyzers()[0])):
            quantile_summaries = state_provider.load(self.additional_analyzers()[0])
        else:
            quantile_func = stateful_approx_quantile(column, relative_error)
            res = data.agg(quantile_func).collect()[0][0]
            quantile_summaries = ApproxQuantileState.quantile_summaries_from_bytes(res)
        median = quantile_summaries.query_by_quantile(0.5)

        mad_col = MAD_COL
        mad = data.withColumn(mad_col, abs(data[column]-median)).approxQuantile(mad_col, [0.5], 0.001)[0]
        lower_bound = median - deviation * mad
        upper_bound = median + deviation * mad
        predicate = "({} < {}) or ({} > {})".format(column, lower_bound, column, upper_bound)
        return data.withColumn(column, when(expr(predicate), value).otherwise(col(column)))

    def replace_by_sma(self, data, state_provider=None):
        column = self.columns[0]
        value = self.values[0]
        deviation = self.model_params.deviation
        radius = self.model_params.radius
        if (state_provider is not None) and (state_provider.has_state(self.additional_analyzers()[0])):
            state = state_provider.load(self.additional_analyzers()[0])
            stddev = state.metric_value()
        else:
            stddev_func = stateful_stddev(conditional_selection(column, None))
            stddev_res = data.agg(stddev_func).collect()[0][0]
            n, _, m2 = stddev_res[0], stddev_res[1], stddev_res[2]
            stddev = math.sqrt(m2 / n)
        bound = stddev * deviation

        window = Window.rangeBetween(-radius, radius)
        sma_col = SMA_COL
        return data.withColumn(sma_col, avg(data[column]).over(window))\
            .withColumn(column, when(_abs(col(sma_col) - col(column)) > bound, value).otherwise(col(column)))\
            .drop(sma_col)

    def replace_by_gmm(self, data):
        k, max_iter, tol, seed = self.model_params.k, self.model_params.max_iter, self.model_params.tol, \
                                 self.model_params.seed
        min_cluster, deviation = self.model_params.min_cluster, self.model_params.deviation
        gmm = GMMOutlierSolver(k=k, max_iter=max_iter, tol=tol, seed=seed, features_col=self.model_params.features_col,
                               probability_col=self.model_params.probability_col,
                               cluster_col=self.model_params.cluster_col, distance_col=self.model_params.distance_col,
                               prediction_col=self.model_params.prediction_col)
        gmm.fit(data, self.columns)
        return gmm.replace(self.values, min_cluster=min_cluster, deviation=deviation)

    def replace_by_kmeans(self, data):
        k, max_iter, seed = self.model_params.k, self.model_params.max_iter, self.model_params.seed
        min_cluster, deviation = self.model_params.min_cluster, self.model_params.deviation
        kmeans = KMeansOutlierSolver(k=k, max_iter=max_iter, seed=seed, features_col=self.model_params.features_col,
                                     cluster_col=self.model_params.cluster_col,
                                     distance_col=self.model_params.distance_col,
                                     prediction_col=self.model_params.prediction_col,
                                     normalize=self.model_params.normalize,
                                     normalized_features_col=self.model_params.normalized_features_col)
        kmeans.fit(data, self.columns)
        return kmeans.replace(self.values, min_cluster, deviation)

    def replace_by_dbscan(self, data):
        eps, min_pts, dist_type, max_partitions = self.model_params.eps, self.model_params.min_pts, \
                                                  self.model_params.dist_type, self.model_params.max_partitions
        dbscan = DBSCANOutlierSolver(eps=eps, min_pts=min_pts, max_partitions=max_partitions, dist_type=dist_type,
                                     cluster_col=self.model_params.cluster_col)
        dbscan.fit(data, self.columns, index_col=self.index_col)
        return dbscan.replace(self.values)

    def replace_by_lof(self, data):
        min_pts, dist_type, contamination, relative_error = self.model_params.min_pts, self.model_params.dist_type, \
                                                            self.model_params.contamination, \
                                                            self.model_params.relative_error
        lof = LOFOutlierSolver(min_pts=min_pts, dist_type=dist_type, features_col=self.model_params.features_col,
                               lof_col=self.model_params.lof_col, prediction_col=self.model_params.prediction_col)
        lof.fit(data, self.columns, index_col=self.index_col)
        return lof.replace(self.values, contamination, relative_error)

    def replace_by_iforest(self, data):
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
        iforest.fit(data, self.columns)
        return iforest.replace(self.values, contamination, relative_error)

    def replace_by_pca(self, data):
        k = self.model_params.k
        method, contamination, relative_error, deviation = self.model_params.method, self.model_params.contamination, \
                                                           self.model_params.relative_error, self.model_params.deviation
        pca = PCAOutlierSolver(k=k, features_col=self.model_params.features_col,
                               standardized_features_col=self.model_params.standardized_features_col,
                               distance_col=self.model_params.distance_col,
                               prediction_col=self.model_params.prediction_col,
                               pca_features_col=self.model_params.pca_features_col)
        pca.fit(data, self.columns)
        return pca.replace(self.values, method=method, contamination=contamination, relative_error=relative_error,
                           deviation=deviation)

    def replace_by_auto_encoder(self, data):
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
        auto_encoder.fit(data, self.columns, self.index_col)
        return auto_encoder.replace(self.values, method, contamination, relative_error, deviation)
